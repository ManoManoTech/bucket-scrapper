//! nginx receiver: dumps each POST body to a host directory for assertion.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use testcontainers::{
    core::{ContainerPort, ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const IMAGE: &str = "nginx";
const TAG: &str = "1.27-alpine";
const PORT: u16 = 80;

/// nginx config that dumps each POST body to /var/dumps/ via `client_body_in_file_only`.
///
/// The `proxy_pass` to an unreachable upstream is a trick to force nginx to fully
/// buffer (and therefore save) the request body before deciding what to do — a plain
/// `return 204` doesn't trigger a body read. The 502 from the failed upstream is
/// caught by `error_page` and rewritten to 204.
///
/// We run nginx as root so the dumped files are owned by root and can be chmodded
/// by `docker exec` before the host reads them. Errors logged at `notice` so the
/// `start worker process` message appears (used as the readiness signal).
const NGINX_CONF: &str = r#"
user root;
worker_processes 1;
error_log /dev/stderr notice;
events { worker_connections 1024; }
http {
    log_format dump '$request_method $uri body=$request_body_file size=$content_length auth=$http_authorization ce=$http_content_encoding ct=$http_content_type';
    access_log /dev/stdout dump;
    error_log /dev/stderr notice;
    client_max_body_size 64m;
    client_body_buffer_size 64m;
    client_body_in_file_only on;
    client_body_temp_path /var/dumps;

    upstream sink { server 127.0.0.1:1; }

    server {
        listen 80;
        location /ingest {
            proxy_pass http://sink;
            proxy_intercept_errors on;
            error_page 502 503 504 = @ok;
        }
        location @ok { return 204; }
    }
}
"#;

pub struct NginxHandle {
    pub container: ContainerAsync<GenericImage>,
    pub url: String,
    pub dump_dir: PathBuf,
}

impl NginxHandle {
    /// Read all dumped bodies. nginx writes them as root with mode 0600, so we
    /// chmod them via `docker exec` before reading from the host.
    pub async fn collect_dumps(&self) -> Result<Vec<Vec<u8>>> {
        // Make all files in the dump dir world-readable. Capital X gives execute
        // only on directories, so files become 0644 not 0755. We also chown to
        // the host uid (matching the bind-mount tempdir owner) — bind-mount uid
        // semantics depend on docker's userns config; chmod is the portable fix.
        let host_uid = unsafe { libc::getuid() };
        let mut exec = self
            .container
            .exec(ExecCommand::new([
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "chmod -R a+rX /var/dumps && chown -R {host_uid}:{host_uid} /var/dumps",
                ),
            ]))
            .await
            .context("chmod/chown /var/dumps in container")?;
        // Drain stdout so the exec actually completes before we read files.
        use tokio::io::AsyncReadExt;
        let mut sink = Vec::new();
        exec.stdout().read_to_end(&mut sink).await.ok();

        let mut entries: Vec<_> = std::fs::read_dir(&self.dump_dir)
            .with_context(|| format!("reading {}", self.dump_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        let mut out = Vec::new();
        for e in entries {
            let p = e.path();
            out.push(
                std::fs::read(&p).with_context(|| format!("reading dump {}", p.display()))?,
            );
        }
        Ok(out)
    }
}

/// Start nginx with the dump directory mounted from `host_dump_dir`.
/// The host directory must already exist and be writable (typically a `tempfile::TempDir`).
pub async fn start_nginx(host_dump_dir: &Path) -> Result<NginxHandle> {
    // The bind-mounted dir needs to be readable by the host user after nginx writes
    // to it; we already chmod the files via `docker exec` in `collect_dumps`, but
    // the dir itself must be writable by the container's nginx (running as root,
    // since we set `user root` in nginx.conf). 0777 is the safest in tests.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(host_dump_dir, std::fs::Permissions::from_mode(0o777))
        .with_context(|| format!("chmod 0777 on {}", host_dump_dir.display()))?;

    let host_dump = host_dump_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", host_dump_dir.display()))?;

    let mount = Mount::bind_mount(host_dump.to_string_lossy().to_string(), "/var/dumps");

    let image = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(ContainerPort::Tcp(PORT))
        .with_wait_for(WaitFor::message_on_stderr("start worker process"));

    let container = image
        .with_copy_to("/etc/nginx/nginx.conf", NGINX_CONF.as_bytes().to_vec())
        .with_mount(mount)
        .start()
        .await
        .context("starting nginx container")?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(PORT.tcp()).await?;
    let url = format!("http://{host}:{port}/ingest");

    Ok(NginxHandle {
        container,
        url,
        dump_dir: host_dump,
    })
}
