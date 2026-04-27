//! Garage S3-compatible single-node test container.

use anyhow::{anyhow, bail, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client as S3Client;
use std::time::Duration;
use testcontainers::{
    core::{ContainerPort, ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::io::AsyncReadExt;

const GARAGE_IMAGE: &str = "dxflrs/garage";
const GARAGE_TAG: &str = "v1.0.1";
const S3_PORT: u16 = 3900;
const RPC_PORT: u16 = 3901;
const ADMIN_PORT: u16 = 3903;

/// Minimal Garage config for a single-node test cluster (no replication).
const GARAGE_TOML: &str = r#"
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "lmdb"

replication_factor = 1

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "0000000000000000000000000000000000000000000000000000000000000000"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "admin-token-test"
metrics_token = "metrics-token-test"
"#;

pub struct GarageHandle {
    pub container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    // Held so the temp config file outlives the container.
    _config_dir: tempfile::TempDir,
}

impl GarageHandle {
    pub fn s3_client(&self) -> S3Client {
        let creds = Credentials::new(
            &self.access_key,
            &self.secret_key,
            None,
            None,
            "garage-test",
        );
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(self.region.clone()))
            .endpoint_url(&self.endpoint)
            .credentials_provider(creds)
            .force_path_style(true)
            .build();
        S3Client::from_conf(conf)
    }

    pub fn env_for_scrapper(&self) -> Vec<(String, String)> {
        vec![
            ("AWS_ENDPOINT_URL_S3".into(), self.endpoint.clone()),
            ("AWS_ACCESS_KEY_ID".into(), self.access_key.clone()),
            ("AWS_SECRET_ACCESS_KEY".into(), self.secret_key.clone()),
            ("AWS_REGION".into(), self.region.clone()),
            ("AWS_S3_FORCE_PATH_STYLE".into(), "true".into()),
        ]
    }
}

/// Boot Garage, apply the cluster layout, create a key + bucket, return credentials.
pub async fn start_garage(bucket_name: &str) -> Result<GarageHandle> {
    // Garage's container image is distroless and has no writable /etc, so we drop the
    // config into a host tempdir and bind-mount the exact file path.
    let config_dir = tempfile::tempdir().context("creating garage config tempdir")?;
    let config_path = config_dir.path().join("garage.toml");
    std::fs::write(&config_path, GARAGE_TOML).context("writing garage.toml")?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o644))?;

    let image = GenericImage::new(GARAGE_IMAGE, GARAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(S3_PORT))
        .with_exposed_port(ContainerPort::Tcp(ADMIN_PORT))
        .with_wait_for(WaitFor::message_on_stderr("S3 API server listening on"));

    let container = image
        .with_mount(Mount::bind_mount(
            config_path.canonicalize()?.to_string_lossy().to_string(),
            "/etc/garage.toml",
        ))
        .start()
        .await
        .context("starting garage container")?;

    // Wait briefly past the "bound" log for the RPC machinery to be ready.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get node id from `garage node id -q` (one-line bare ID with -q).
    let node_id = exec_capture(&container, &["/garage", "node", "id", "-q"])
        .await
        .context("reading garage node id")?;
    // `node id -q` prints `<id>@<host>:<port>`; strip the `@…` suffix.
    let node_id = node_id
        .trim()
        .split('@')
        .next()
        .ok_or_else(|| anyhow!("empty node id"))?
        .to_string();
    if node_id.is_empty() {
        bail!("garage returned empty node id");
    }

    exec_check(
        &container,
        &[
            "/garage", "layout", "assign", "-z", "dc1", "-c", "1G", &node_id,
        ],
    )
    .await
    .context("garage layout assign")?;
    exec_check(
        &container,
        &["/garage", "layout", "apply", "--version", "1"],
    )
    .await
    .context("garage layout apply")?;

    // Wait until the cluster reports the layout as effective. `garage status` exits 0
    // once the node is healthy under the applied layout.
    // After applying layout, give Garage a moment to converge before key creation.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a key and parse credentials from stdout. (`garage key create <name>`)
    let key_out = exec_capture(&container, &["/garage", "key", "create", "testkey"]).await?;
    let (access_key, secret_key) = parse_key_create(&key_out)
        .ok_or_else(|| anyhow!("could not parse `garage key create` output:\n{key_out}"))?;

    exec_check(&container, &["/garage", "bucket", "create", bucket_name]).await?;
    exec_check(
        &container,
        &[
            "/garage",
            "bucket",
            "allow",
            "--read",
            "--write",
            bucket_name,
            "--key",
            "testkey",
        ],
    )
    .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(S3_PORT.tcp()).await?;
    let endpoint = format!("http://{host}:{port}");

    Ok(GarageHandle {
        container,
        endpoint,
        access_key,
        secret_key,
        region: "garage".to_string(),
        _config_dir: config_dir,
    })
}

fn parse_key_create(stdout: &str) -> Option<(String, String)> {
    let mut access = None;
    let mut secret = None;
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Key ID:") {
            access = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("Secret access key:") {
            secret = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("Secret key:") {
            secret = Some(rest.trim().to_string());
        }
    }
    Some((access?, secret?))
}

async fn exec_capture(container: &ContainerAsync<GenericImage>, cmd: &[&str]) -> Result<String> {
    let mut result = container
        .exec(ExecCommand::new(cmd.iter().map(|s| s.to_string())))
        .await
        .with_context(|| format!("exec {cmd:?}"))?;
    let mut buf = Vec::new();
    result.stdout().read_to_end(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

async fn exec_check(container: &ContainerAsync<GenericImage>, cmd: &[&str]) -> Result<()> {
    let stdout = exec_capture(container, cmd).await?;
    // We rely on exec returning Ok when the process exits zero; inspect stdout
    // only for hard error markers we know garage uses.
    if stdout.to_lowercase().contains("error:") {
        bail!("command {cmd:?} reported error:\n{stdout}");
    }
    Ok(())
}
