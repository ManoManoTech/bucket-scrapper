//! End-to-end test of the runtime control plane over a real Unix domain
//! socket: a `serve()` task driven by a client that sends each command kind
//! and asserts the reply + the side effect on the shared state.
//!
//! This drives the control server with synthetic handles instead of the full
//! S3 pipeline, so it is fast and hermetic while still exercising the real
//! socket transport, JSON protocol, and apply/status logic.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bucket_scrapper::control::server::{serve, ControlContext, StatusHandles};
use bucket_scrapper::control::{encode_request, ControlRequest, ControlResponse, RuntimeControls};
use bucket_scrapper::pipeline::observer::{ChannelObserver, DownloadObserver, ReadPathMetrics};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

struct Harness {
    socket: std::path::PathBuf,
    controls: Arc<RuntimeControls>,
    grow_rx: flume::Receiver<usize>,
    _tmp: tempfile::TempDir,
    // Kept alive so the line-channel observer reports a live len/cap.
    _line_tx: flume::Sender<u8>,
    _server: tokio::task::JoinHandle<()>,
}

async fn start(file_slots: usize, range: usize, chunk_bytes: usize, workers: usize) -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let socket = tmp.path().join("bs.sock");

    let controls = RuntimeControls::new(file_slots, range, chunk_bytes);
    let workers_alive = Arc::new(AtomicUsize::new(workers));
    let (grow_tx, grow_rx) = flume::unbounded::<usize>();
    let (line_tx, _line_rx) = flume::bounded::<u8>(1000);

    let ctx = ControlContext::new(
        controls.clone(),
        grow_tx,
        StatusHandles {
            workers_alive: workers_alive.clone(),
            metrics: ReadPathMetrics::new(0, None),
            download_observer: DownloadObserver::new(),
            filter_bytes_in: Arc::new(AtomicUsize::new(0)),
            match_count: Arc::new(AtomicUsize::new(0)),
            workers_in_ingest: Arc::new(AtomicUsize::new(0)),
            sink_obs: bucket_scrapper::pipeline::SinkObservability::default(),
            sink_kind: "void",
            line_channel: ChannelObserver::from_sender(&line_tx),
            line_buffer_size: 1000,
        },
    );

    let socket_for_task = socket.clone();
    let server = tokio::spawn(async move {
        let _ = serve(socket_for_task, ctx).await;
    });

    // Wait for the socket to appear / accept connections.
    for _ in 0..50 {
        if UnixStream::connect(&socket).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Harness {
        socket,
        controls,
        grow_rx,
        _tmp: tmp,
        _line_tx: line_tx,
        _server: server,
    }
}

/// Send one request, read one response.
async fn call(socket: &std::path::Path, req: ControlRequest) -> ControlResponse {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = encode_request(&req);
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();

    let mut lines = BufReader::new(read_half).lines();
    let resp = lines.next_line().await.unwrap().expect("a response line");
    serde_json::from_str(&resp).expect("parse response")
}

#[tokio::test]
async fn status_reports_startup_values() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::Status).await;
    match resp {
        ControlResponse::Status(s) => {
            assert_eq!(s.filter_workers_alive, 8);
            assert_eq!(s.download_tasks_limit, 32);
            assert_eq!(s.range_concurrency_limit, 16);
            assert_eq!(s.part_size_mb, 0);
            assert_eq!(s.line_buffer_size, 1000);
            assert_eq!(s.line_channel_cap, 1000);
        }
        other => panic!("expected Status, got {other:?}"),
    }
}

#[tokio::test]
async fn grow_filter_workers_signals_join_loop() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::AdjustFilterWorkers { delta: 2 }).await;
    match resp {
        ControlResponse::Applied {
            knob,
            before,
            after,
            ..
        } => {
            assert_eq!(knob, "filter_workers");
            assert_eq!(before, 8);
            assert_eq!(after, 10);
        }
        other => panic!("expected Applied, got {other:?}"),
    }
    // The join loop would receive the grow request.
    assert_eq!(h.grow_rx.recv_async().await.unwrap(), 2);
}

#[tokio::test]
async fn shrink_filter_workers_posts_retirements_and_clamps() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::AdjustFilterWorkers { delta: -3 }).await;
    match resp {
        ControlResponse::Applied { before, after, .. } => {
            assert_eq!(before, 8);
            assert_eq!(after, 5);
        }
        other => panic!("expected Applied, got {other:?}"),
    }
    assert_eq!(h.controls.filter_retire_pending(), 3);

    // Floor: never below one survivor. Effective target is now 5 (8 alive − 3
    // queued), so a huge shrink claims 4 more, landing the target at 1.
    let resp = call(
        &h.socket,
        ControlRequest::AdjustFilterWorkers { delta: -100 },
    )
    .await;
    if let ControlResponse::Applied { before, after, .. } = resp {
        assert_eq!(before, 5);
        assert_eq!(after, 1);
    } else {
        panic!("expected Applied");
    }
    assert_eq!(h.controls.filter_retire_pending(), 7);
}

#[tokio::test]
async fn adjust_download_tasks_changes_limit() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::AdjustDownloadTasks { delta: 4 }).await;
    if let ControlResponse::Applied { before, after, .. } = resp {
        assert_eq!(before, 32);
        assert_eq!(after, 36);
    } else {
        panic!("expected Applied, got {resp:?}");
    }
    assert_eq!(h.controls.file_limit(), 36);

    let resp = call(
        &h.socket,
        ControlRequest::AdjustRangeConcurrency { delta: -8 },
    )
    .await;
    if let ControlResponse::Applied { after, .. } = resp {
        assert_eq!(after, 8);
    } else {
        panic!("expected Applied");
    }
    assert_eq!(h.controls.range_limit(), 8);
}

#[tokio::test]
async fn set_part_size_applies() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::SetPartSizeMb { mb: 16 }).await;
    if let ControlResponse::Applied { before, after, .. } = resp {
        assert_eq!(before, 0);
        assert_eq!(after, 16);
    } else {
        panic!("expected Applied, got {resp:?}");
    }
    assert_eq!(h.controls.part_size_mb(), 16);
    assert_eq!(h.controls.chunk_size.load(Ordering::Relaxed), 16_000_000);
}

#[tokio::test]
async fn set_line_buffer_is_unsupported() {
    let h = start(32, 16, 0, 8).await;
    let resp = call(&h.socket, ControlRequest::SetLineBufferSize { size: 2000 }).await;
    assert!(
        matches!(resp, ControlResponse::Unsupported(_)),
        "expected Unsupported, got {resp:?}"
    );
}

#[tokio::test]
async fn bad_request_line_yields_error_response() {
    let h = start(32, 16, 0, 8).await;
    let stream = UnixStream::connect(&h.socket).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(b"this is not json\n").await.unwrap();
    write_half.flush().await.unwrap();
    let mut lines = BufReader::new(read_half).lines();
    let resp: ControlResponse =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(matches!(resp, ControlResponse::Error(_)), "got {resp:?}");
}
