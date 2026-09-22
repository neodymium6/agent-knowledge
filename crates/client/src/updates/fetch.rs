use std::time::Duration;

use serde::Deserialize;

use super::{CheckError, Release, stable_version};

const ENDPOINT: &str = "https://api.github.com/repos/neodymium6/agent-knowledge/releases/latest";
const TIMEOUT: Duration = Duration::from_secs(3);
const MAXIMUM_RESPONSE_BYTES: usize = 256 * 1024;

pub(super) fn latest() -> Result<Release, CheckError> {
    // Avoid runtime shutdown waiting for a stalled system DNS resolver thread.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| CheckError::Network)?;
    let result = runtime.block_on(async {
        tokio::time::timeout(TIMEOUT, request(ENDPOINT, true, TIMEOUT))
            .await
            .map_err(|_| CheckError::Network)?
    });
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

async fn request(
    endpoint: &str,
    https_only: bool,
    timeout: Duration,
) -> Result<Release, CheckError> {
    // reqwest's no-provider build requires an explicit provider; an existing one is also valid.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder = reqwest::Client::builder()
        .https_only(https_only)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(timeout)
        .timeout(timeout)
        .tls_sslkeylogfile(false)
        .user_agent("agent-knowledge-release-check");
    // Local HTTP fixtures need no platform CA installation in a Nix build sandbox.
    // Empty trust rejects every TLS certificate; verification is never disabled.
    #[cfg(test)]
    let builder = if https_only {
        builder
    } else {
        builder.tls_certs_only([])
    };
    let client = builder.build().map_err(|_| CheckError::Network)?;
    let mut response = client
        .get(endpoint)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2026-03-10")
        .send()
        .await
        .map_err(|_| CheckError::Network)?;
    match response.status().as_u16() {
        200 => (),
        403 | 429 => return Err(CheckError::RateLimited),
        404 => return Err(CheckError::NoStableRelease),
        _ => return Err(CheckError::Http),
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
    {
        return Err(CheckError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| CheckError::Network)? {
        if chunk.len() > MAXIMUM_RESPONSE_BYTES.saturating_sub(bytes.len()) {
            return Err(CheckError::InvalidResponse);
        }
        bytes.extend_from_slice(&chunk);
    }
    decode(&bytes)
}

#[derive(Deserialize)]
struct Metadata {
    tag_name: String,
    html_url: String,
    draft: bool,
    prerelease: bool,
}

fn decode(bytes: &[u8]) -> Result<Release, CheckError> {
    let metadata: Metadata =
        serde_json::from_slice(bytes).map_err(|_| CheckError::InvalidResponse)?;
    if metadata.draft || metadata.prerelease {
        return Err(CheckError::NoStableRelease);
    }
    let version = metadata
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&metadata.tag_name);
    if stable_version(version).is_none() {
        return Err(CheckError::InvalidResponse);
    }
    let release = Release {
        version: version.to_owned(),
        url: metadata.html_url,
    };
    if !release.valid() {
        return Err(CheckError::InvalidResponse);
    }
    Ok(release)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn metadata() -> serde_json::Value {
        serde_json::json!({"tag_name":"v9.8.7","html_url":"https://github.com/neodymium6/agent-knowledge/releases/tag/v9.8.7","draft":false,"prerelease":false})
    }

    #[test]
    fn requires_a_stable_release_and_the_exact_public_repository() {
        let valid = metadata();
        assert!(decode(valid.to_string().as_bytes()).is_ok());
        for (field, value) in [
            ("draft", serde_json::json!(true)),
            ("prerelease", serde_json::json!(true)),
            ("tag_name", serde_json::json!("v9.8.7-rc.1")),
            ("html_url", serde_json::json!("https://fictional.invalid/")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(decode(invalid.to_string().as_bytes()).is_err());
        }
        assert!(decode(b"{}").is_err());
        assert!(decode(b"not JSON").is_err());
    }

    async fn mock(response: String, delay: Duration) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .unwrap_or_else(|e| panic!("fixture: {e}"));
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") && bytes.len() < 8192 {
                bytes.push(
                    socket
                        .read_u8()
                        .await
                        .unwrap_or_else(|e| panic!("request: {e}")),
                );
            }
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response.as_bytes()).await;
            String::from_utf8(bytes).unwrap_or_else(|e| panic!("request: {e}"))
        });
        (
            format!("http://{address}/repos/neodymium6/agent-knowledge/releases/latest"),
            task,
        )
    }

    #[tokio::test]
    async fn fetches_only_public_metadata_with_no_authentication_or_redirects() {
        let body = metadata().to_string();
        let (endpoint, server) = mock(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
            Duration::ZERO,
        )
        .await;
        let release = request(&endpoint, false, TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("fetch: {e:?}"));
        assert_eq!(release.version, "9.8.7");
        let request = server
            .await
            .unwrap_or_else(|e| panic!("server: {e}"))
            .to_ascii_lowercase();
        assert!(
            request
                .starts_with("get /repos/neodymium6/agent-knowledge/releases/latest http/1.1\r\n")
        );
        assert!(!request.contains("authorization") && !request.contains("cookie"));
        assert!(request.contains("user-agent: agent-knowledge-release-check\r\n"));
        let (endpoint, server) = mock("HTTP/1.1 302 Found\r\nLocation: https://fictional.invalid/\r\nContent-Length: 0\r\n\r\n".to_owned(), Duration::ZERO).await;
        assert_eq!(
            super::request(&endpoint, false, TIMEOUT).await.err(),
            Some(CheckError::Http)
        );
        server.await.unwrap_or_else(|e| panic!("server: {e}"));
    }

    #[tokio::test]
    async fn bounds_response_size_deadlines_and_http_failures() {
        for (response, error) in [
            (
                "HTTP/1.1 429 Limited\r\nContent-Length: 0\r\n\r\n".to_owned(),
                CheckError::RateLimited,
            ),
            (
                "HTTP/1.1 404 Missing\r\nContent-Length: 0\r\n\r\n".to_owned(),
                CheckError::NoStableRelease,
            ),
            (
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                    MAXIMUM_RESPONSE_BYTES + 1
                ),
                CheckError::InvalidResponse,
            ),
            (
                format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}",
                    "x".repeat(MAXIMUM_RESPONSE_BYTES + 1)
                ),
                CheckError::InvalidResponse,
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_owned(),
                CheckError::InvalidResponse,
            ),
        ] {
            let (endpoint, server) = mock(response, Duration::ZERO).await;
            assert_eq!(request(&endpoint, false, TIMEOUT).await.err(), Some(error));
            server.await.unwrap_or_else(|e| panic!("server: {e}"));
        }
        let (endpoint, server) = mock(String::new(), Duration::from_millis(400)).await;
        let start = std::time::Instant::now();
        assert_eq!(
            request(&endpoint, false, Duration::from_millis(100))
                .await
                .err(),
            Some(CheckError::Network)
        );
        assert!(start.elapsed() < Duration::from_secs(2));
        server.abort();
    }
}
