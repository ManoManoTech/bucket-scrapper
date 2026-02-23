use tracing::info;

/// Resolve a custom CA bundle path for TLS verification through proxies.
///
/// Priority:
/// 1. `AWS_CA_BUNDLE` env var (explicit override)
/// 2. Auto-detect `~/.mitmproxy/mitmproxy-ca-cert.pem` when any proxy env var is set
pub fn resolve_ca_bundle_path() -> Option<String> {
    if let Ok(path) = std::env::var("AWS_CA_BUNDLE") {
        return Some(path);
    }

    let has_proxy = std::env::var_os("HTTPS_PROXY").is_some()
        || std::env::var_os("https_proxy").is_some()
        || std::env::var_os("HTTP_PROXY").is_some()
        || std::env::var_os("http_proxy").is_some()
        || std::env::var_os("ALL_PROXY").is_some()
        || std::env::var_os("all_proxy").is_some();

    if !has_proxy {
        return None;
    }

    let home = std::env::var("HOME").ok()?;
    let path = format!("{home}/.mitmproxy/mitmproxy-ca-cert.pem");
    if std::path::Path::new(&path).exists() {
        info!(path = %path, "Auto-detected mitmproxy CA");
        Some(path)
    } else {
        None
    }
}
