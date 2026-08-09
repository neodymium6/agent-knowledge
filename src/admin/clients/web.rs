use std::fmt;
use std::fs;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use agent_knowledge_access::{
    AccessRegistry, AccessRegistryError, ClientStatus, MutationOutcome, NormalizedPublicKey,
    RegistrySnapshot,
};
use agent_knowledge_protocol::ClientId;
use askama::Template;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Form, Path, Query, State};
use axum::http::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HeaderName, HeaderValue, REFERRER_POLICY,
    X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use nix::errno::Errno;
use nix::sys::socket::{AddressFamily, SockFlag, SockType, UnixAddr, connect, socket};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use ulid::Ulid;

const WEB_ADMIN_ACTOR: &str = "admin-web";
const MAXIMUM_FORM_BYTES: usize = 32 * 1024;
const STYLESHEET: &str = include_str!("../../../templates/admin/clients.css");
const PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");

#[derive(Clone)]
struct WebState {
    registry: AccessRegistry,
    csrf_token: Arc<str>,
}

#[derive(Deserialize)]
struct IndexQuery {
    notice: Option<String>,
}

#[derive(Deserialize)]
struct AddForm {
    csrf_token: String,
    client_id: String,
    public_key: String,
}

#[derive(Deserialize)]
struct MutationForm {
    csrf_token: String,
}

#[derive(Deserialize)]
struct RotateKeyForm {
    csrf_token: String,
    expected_fingerprint: String,
    public_key: String,
}

#[derive(Template)]
#[template(path = "admin/clients.html")]
struct ClientsTemplate {
    csrf_token: Arc<str>,
    generation_id: Option<String>,
    clients: Vec<ClientView>,
    notice: &'static str,
    error: String,
}

struct ClientView {
    client_id: String,
    fingerprint: String,
    active: bool,
}

pub(super) fn run(registry_root: PathBuf, socket_path: PathBuf) -> Result<(), ClientAdminWebError> {
    validate_socket_path(&socket_path)?;
    let registry = AccessRegistry::open_for_effective_user(registry_root)
        .map_err(ClientAdminWebError::Registry)?;
    let state = WebState {
        registry,
        csrf_token: Arc::from(new_csrf_token()),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(ClientAdminWebError::Runtime)?
        .block_on(serve(state, socket_path))
}

async fn serve(state: WebState, socket_path: PathBuf) -> Result<(), ClientAdminWebError> {
    let listener = bind_listener(&socket_path).map_err(|source| ClientAdminWebError::Bind {
        path: socket_path.clone(),
        source,
    })?;
    let bound_socket = BoundSocket::new(socket_path)?;
    bound_socket.set_permissions()?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(wait_for_shutdown())
        .await
        .map_err(ClientAdminWebError::Serve)
}

fn validate_socket_path(socket_path: &FsPath) -> Result<(), ClientAdminWebError> {
    if !socket_path.is_absolute() || socket_path.file_name().is_none() {
        return Err(ClientAdminWebError::InvalidSocketPath(
            socket_path.to_path_buf(),
        ));
    }
    let parent = socket_path
        .parent()
        .ok_or_else(|| ClientAdminWebError::InvalidSocketPath(socket_path.to_path_buf()))?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|source| ClientAdminWebError::SocketDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    if canonical_parent != parent {
        return Err(ClientAdminWebError::InsecureSocketDirectory(
            parent.to_path_buf(),
        ));
    }

    let effective_uid = nix::unistd::Uid::effective().as_raw();
    for ancestor in parent.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|source| {
            ClientAdminWebError::SocketDirectory {
                path: ancestor.to_path_buf(),
                source,
            }
        })?;
        let trusted_owner = metadata.uid() == 0 || metadata.uid() == effective_uid;
        if !metadata.file_type().is_dir()
            || !trusted_owner
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(ClientAdminWebError::InsecureSocketDirectory(
                ancestor.to_path_buf(),
            ));
        }
    }
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|source| ClientAdminWebError::SocketDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    if parent_metadata.uid() != effective_uid {
        return Err(ClientAdminWebError::InsecureSocketDirectory(
            parent.to_path_buf(),
        ));
    }

    clear_stale_socket(socket_path)
}

fn clear_stale_socket(socket_path: &FsPath) -> Result<(), ClientAdminWebError> {
    let observed = match fs::symlink_metadata(socket_path) {
        Ok(metadata) if metadata.file_type().is_socket() => metadata,
        Ok(_) => {
            return Err(ClientAdminWebError::SocketAlreadyExists(
                socket_path.to_path_buf(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ClientAdminWebError::SocketPath {
                path: socket_path.to_path_buf(),
                source,
            });
        }
    };

    match probe_socket(socket_path)? {
        SocketProbe::Live => Err(ClientAdminWebError::SocketAlreadyExists(
            socket_path.to_path_buf(),
        )),
        SocketProbe::Missing => Ok(()),
        SocketProbe::Stale => {
            let current = match fs::symlink_metadata(socket_path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(source) => {
                    return Err(ClientAdminWebError::SocketPath {
                        path: socket_path.to_path_buf(),
                        source,
                    });
                }
            };
            if !current.file_type().is_socket()
                || current.dev() != observed.dev()
                || current.ino() != observed.ino()
            {
                return Err(ClientAdminWebError::SocketAlreadyExists(
                    socket_path.to_path_buf(),
                ));
            }
            fs::remove_file(socket_path).map_err(|source| ClientAdminWebError::SocketPath {
                path: socket_path.to_path_buf(),
                source,
            })
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SocketProbe {
    Live,
    Stale,
    Missing,
}

fn probe_socket(socket_path: &FsPath) -> Result<SocketProbe, ClientAdminWebError> {
    let descriptor = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
        None,
    )
    .map_err(|error| socket_error(socket_path, error))?;
    let address = UnixAddr::new(socket_path).map_err(|error| socket_error(socket_path, error))?;
    match connect(descriptor.as_raw_fd(), &address) {
        Ok(()) => Ok(SocketProbe::Live),
        Err(Errno::ECONNREFUSED) => Ok(SocketProbe::Stale),
        Err(Errno::ENOENT) => Ok(SocketProbe::Missing),
        Err(Errno::EAGAIN | Errno::EINPROGRESS | Errno::EALREADY) => Ok(SocketProbe::Live),
        Err(error) => Err(socket_error(socket_path, error)),
    }
}

fn socket_error(socket_path: &FsPath, error: Errno) -> ClientAdminWebError {
    ClientAdminWebError::SocketPath {
        path: socket_path.to_path_buf(),
        source: std::io::Error::from_raw_os_error(error as i32),
    }
}

#[cfg(not(test))]
fn bind_listener(socket_path: &FsPath) -> std::io::Result<tokio::net::UnixListener> {
    let previous_umask = nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(0o117));
    let listener = tokio::net::UnixListener::bind(socket_path);
    nix::sys::stat::umask(previous_umask);
    listener
}

#[cfg(test)]
fn bind_listener(socket_path: &FsPath) -> std::io::Result<tokio::net::UnixListener> {
    tokio::net::UnixListener::bind(socket_path)
}

struct BoundSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl BoundSocket {
    fn new(path: PathBuf) -> Result<Self, ClientAdminWebError> {
        let metadata =
            fs::symlink_metadata(&path).map_err(|source| ClientAdminWebError::SocketPath {
                path: path.clone(),
                source,
            })?;
        if !metadata.file_type().is_socket() {
            return Err(ClientAdminWebError::InvalidBoundSocket(path));
        }
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn set_permissions(&self) -> Result<(), ClientAdminWebError> {
        fs::set_permissions(&self.path, fs::Permissions::from_mode(0o660)).map_err(|source| {
            ClientAdminWebError::SocketPath {
                path: self.path.clone(),
                source,
            }
        })
    }
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/clients.css", get(stylesheet))
        .route("/healthz", get(health))
        .route("/clients", post(add_client))
        .route("/clients/{client_id}/disable", post(disable_client))
        .route("/clients/{client_id}/enable", post(enable_client))
        .route("/clients/{client_id}/rotate-key", post(rotate_key))
        .layer(DefaultBodyLimit::max(MAXIMUM_FORM_BYTES))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

async fn index(State(state): State<WebState>, Query(query): Query<IndexQuery>) -> Response {
    match read_current(&state).await {
        Ok(snapshot) => render_page(
            &state,
            snapshot,
            notice_text(query.notice.as_deref()),
            String::new(),
            StatusCode::OK,
        ),
        Err(error) => render_page(
            &state,
            None,
            "",
            error.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn health(State(state): State<WebState>) -> StatusCode {
    match read_current(&state).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn stylesheet() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/css; charset=utf-8")], STYLESHEET)
}

async fn add_client(State(state): State<WebState>, Form(form): Form<AddForm>) -> Response {
    if !csrf_matches(&state.csrf_token, &form.csrf_token) {
        return render_failure(&state, StatusCode::FORBIDDEN, "invalid CSRF token").await;
    }
    let client_id = match form.client_id.parse::<ClientId>() {
        Ok(client_id) => client_id,
        Err(error) => {
            return render_failure(&state, StatusCode::BAD_REQUEST, &error.to_string()).await;
        }
    };
    let public_key = match NormalizedPublicKey::parse(&form.public_key) {
        Ok(public_key) => public_key,
        Err(error) => {
            return render_failure(&state, StatusCode::BAD_REQUEST, &error.to_string()).await;
        }
    };
    match mutate(&state, move |registry| {
        registry.add(client_id, public_key, WEB_ADMIN_ACTOR)
    })
    .await
    {
        Ok(outcome) => mutation_redirect(&outcome, "added").into_response(),
        Err(error) => mutation_failure(&state, error).await,
    }
}

async fn disable_client(
    State(state): State<WebState>,
    Path(client_id): Path<String>,
    Form(form): Form<MutationForm>,
) -> Response {
    change_status(state, client_id, form.csrf_token, false).await
}

async fn enable_client(
    State(state): State<WebState>,
    Path(client_id): Path<String>,
    Form(form): Form<MutationForm>,
) -> Response {
    change_status(state, client_id, form.csrf_token, true).await
}

async fn change_status(
    state: WebState,
    client_id: String,
    csrf_token: String,
    enable: bool,
) -> Response {
    if !csrf_matches(&state.csrf_token, &csrf_token) {
        return render_failure(&state, StatusCode::FORBIDDEN, "invalid CSRF token").await;
    }
    let client_id = match client_id.parse::<ClientId>() {
        Ok(client_id) => client_id,
        Err(error) => {
            return render_failure(&state, StatusCode::BAD_REQUEST, &error.to_string()).await;
        }
    };
    match mutate(&state, move |registry| {
        if enable {
            registry.enable(&client_id, WEB_ADMIN_ACTOR)
        } else {
            registry.disable(&client_id, WEB_ADMIN_ACTOR)
        }
    })
    .await
    {
        Ok(outcome) => {
            mutation_redirect(&outcome, if enable { "enabled" } else { "disabled" }).into_response()
        }
        Err(error) => mutation_failure(&state, error).await,
    }
}

async fn rotate_key(
    State(state): State<WebState>,
    Path(client_id): Path<String>,
    Form(form): Form<RotateKeyForm>,
) -> Response {
    if !csrf_matches(&state.csrf_token, &form.csrf_token) {
        return render_failure(&state, StatusCode::FORBIDDEN, "invalid CSRF token").await;
    }
    let client_id = match client_id.parse::<ClientId>() {
        Ok(client_id) => client_id,
        Err(error) => {
            return render_failure(&state, StatusCode::BAD_REQUEST, &error.to_string()).await;
        }
    };
    let public_key = match NormalizedPublicKey::parse(&form.public_key) {
        Ok(public_key) => public_key,
        Err(error) => {
            return render_failure(&state, StatusCode::BAD_REQUEST, &error.to_string()).await;
        }
    };
    let expected_fingerprint = form.expected_fingerprint;
    match mutate(&state, move |registry| {
        registry.rotate_key(
            &client_id,
            &expected_fingerprint,
            public_key,
            WEB_ADMIN_ACTOR,
        )
    })
    .await
    {
        Ok(outcome) => mutation_redirect(&outcome, "rotated").into_response(),
        Err(error) => mutation_failure(&state, error).await,
    }
}

async fn read_current(state: &WebState) -> Result<Option<RegistrySnapshot>, WebOperationError> {
    let registry = state.registry.clone();
    tokio::task::spawn_blocking(move || registry.current())
        .await
        .map_err(WebOperationError::Task)?
        .map_err(WebOperationError::Registry)
}

async fn mutate(
    state: &WebState,
    operation: impl FnOnce(AccessRegistry) -> Result<MutationOutcome, AccessRegistryError>
    + Send
    + 'static,
) -> Result<MutationOutcome, WebOperationError> {
    let registry = state.registry.clone();
    tokio::task::spawn_blocking(move || operation(registry))
        .await
        .map_err(WebOperationError::Task)?
        .map_err(WebOperationError::Registry)
}

async fn render_failure(state: &WebState, status: StatusCode, message: &str) -> Response {
    let snapshot = read_current(state).await.ok().flatten();
    render_page(state, snapshot, "", message.to_owned(), status)
}

async fn mutation_failure(state: &WebState, error: WebOperationError) -> Response {
    let status = error.http_status();
    render_failure(state, status, &error.to_string()).await
}

fn mutation_redirect(outcome: &MutationOutcome, notice: &'static str) -> impl IntoResponse {
    if outcome.changed() {
        Redirect::to(&format!("/?notice={notice}"))
    } else {
        Redirect::to("/?notice=unchanged")
    }
}

fn render_page(
    state: &WebState,
    snapshot: Option<RegistrySnapshot>,
    notice: &'static str,
    error: String,
    status: StatusCode,
) -> Response {
    let generation_id = snapshot
        .as_ref()
        .map(|snapshot| snapshot.generation_id().to_owned());
    let clients = snapshot.map_or_else(Vec::new, |snapshot| {
        snapshot
            .clients()
            .iter()
            .map(|client| ClientView {
                client_id: client.client_id().to_string(),
                fingerprint: client.fingerprint().to_owned(),
                active: client.status() == ClientStatus::Active,
            })
            .collect()
    });
    let template = ClientsTemplate {
        csrf_token: state.csrf_token.clone(),
        generation_id,
        clients,
        notice,
        error,
    };
    match template.render() {
        Ok(body) => (status, Html(body)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not render administration page",
        )
            .into_response(),
    }
}

fn notice_text(notice: Option<&str>) -> &'static str {
    match notice {
        Some("added") => "Client added.",
        Some("disabled") => "Client disabled.",
        Some("enabled") => "Client enabled.",
        Some("rotated") => "Client key rotated.",
        Some("unchanged") => "No registry change was needed.",
        _ => "",
    }
}

fn new_csrf_token() -> String {
    let seed = format!("{}{}", Ulid::generate(), Ulid::generate());
    URL_SAFE_NO_PAD.encode(Sha256::digest(seed.as_bytes()))
}

fn csrf_matches(expected: &str, supplied: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let supplied = Sha256::digest(supplied.as_bytes());
    expected
        .iter()
        .zip(supplied.iter())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

async fn security_headers(request: Request<axum::body::Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        PERMISSIONS_POLICY,
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Debug)]
enum WebOperationError {
    Registry(AccessRegistryError),
    Task(tokio::task::JoinError),
}

impl WebOperationError {
    fn http_status(&self) -> StatusCode {
        match self {
            Self::Registry(
                AccessRegistryError::Busy
                | AccessRegistryError::ClientAlreadyExists(_)
                | AccessRegistryError::UnknownClient(_)
                | AccessRegistryError::KeyAlreadyRegistered(_)
                | AccessRegistryError::FingerprintConflict { .. },
            ) => StatusCode::CONFLICT,
            Self::Registry(_) | Self::Task(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for WebOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => error.fmt(formatter),
            Self::Task(error) => write!(
                formatter,
                "administration operation stopped unexpectedly: {error}"
            ),
        }
    }
}

/// Failure to run the optional local client-administration Web UI.
#[derive(Debug)]
pub(crate) enum ClientAdminWebError {
    InvalidSocketPath(PathBuf),
    InsecureSocketDirectory(PathBuf),
    SocketAlreadyExists(PathBuf),
    InvalidBoundSocket(PathBuf),
    SocketDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    SocketPath {
        path: PathBuf,
        source: std::io::Error,
    },
    Registry(AccessRegistryError),
    Runtime(std::io::Error),
    Bind {
        path: PathBuf,
        source: std::io::Error,
    },
    Serve(std::io::Error),
}

impl fmt::Display for ClientAdminWebError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSocketPath(path) => write!(
                formatter,
                "client administration socket path must be absolute and name one socket: {}",
                path.display()
            ),
            Self::InsecureSocketDirectory(path) => write!(
                formatter,
                "client administration socket directory is not private and trusted: {}",
                path.display()
            ),
            Self::SocketAlreadyExists(path) => write!(
                formatter,
                "client administration socket path already exists: {}",
                path.display()
            ),
            Self::InvalidBoundSocket(path) => write!(
                formatter,
                "client administration listener did not create a socket: {}",
                path.display()
            ),
            Self::SocketDirectory { path, source } => write!(
                formatter,
                "could not inspect client administration socket directory {}: {source}",
                path.display()
            ),
            Self::SocketPath { path, source } => write!(
                formatter,
                "could not manage client administration socket {}: {source}",
                path.display()
            ),
            Self::Registry(error) => error.fmt(formatter),
            Self::Runtime(error) => {
                write!(
                    formatter,
                    "could not start client administration runtime: {error}"
                )
            }
            Self::Bind { path, source } => write!(
                formatter,
                "could not bind client administration listener at {}: {source}",
                path.display()
            ),
            Self::Serve(error) => write!(formatter, "client administration server failed: {error}"),
        }
    }
}

impl std::error::Error for ClientAdminWebError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidSocketPath(_)
            | Self::InsecureSocketDirectory(_)
            | Self::SocketAlreadyExists(_)
            | Self::InvalidBoundSocket(_) => None,
            Self::SocketDirectory { source, .. } | Self::SocketPath { source, .. } => Some(source),
            Self::Registry(error) => Some(error),
            Self::Runtime(error) | Self::Serve(error) => Some(error),
            Self::Bind { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::body::{Body, to_bytes};
    use axum::http::header::{CONTENT_SECURITY_POLICY, CONTENT_TYPE, LOCATION};
    use tower::ServiceExt as _;

    use super::*;

    const KEY_A: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN796jTiQfZfG1KaT0PtFDJ/XFSqti fictional-a@example.invalid";
    const KEY_B: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKWz8j8C3gyf7u8sVvD6cGx0iW9F8uQ5yT6u7V8wX9yZ fictional-b@example.invalid";
    const TEST_TOKEN: &str = "fictional-test-csrf-token";
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "agent-knowledge-admin-web-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path)
                .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("test directory must be removed: {error}");
            }
        }
    }

    fn test_state(root: &Path) -> WebState {
        let registry = AccessRegistry::open_for_effective_user(root.join("registry"))
            .unwrap_or_else(|error| panic!("test registry must open: {error}"));
        WebState {
            registry,
            csrf_token: Arc::from(TEST_TOKEN),
        }
    }

    fn encoded_form(fields: &[(&str, &str)]) -> String {
        fields
            .iter()
            .map(|(name, value)| format!("{}={}", encode_component(name), encode_component(value)))
            .collect::<Vec<_>>()
            .join("&")
    }

    fn encode_component(value: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut encoded = String::new();
        for byte in value.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                encoded.push(char::from(byte));
            } else {
                encoded.push('%');
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
        encoded
    }

    async fn request(app: Router, method: &str, uri: &str, body: String) -> Response {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap_or_else(|error| panic!("test request must build: {error}"));
        app.oneshot(request)
            .await
            .unwrap_or_else(|error| match error {})
    }

    #[tokio::test]
    async fn renders_registry_with_security_headers() {
        let root = TestDirectory::create();
        let state = test_state(root.path());
        state
            .registry
            .add(
                "fictional-node-a"
                    .parse()
                    .unwrap_or_else(|error| panic!("test client ID must parse: {error}")),
                NormalizedPublicKey::parse(KEY_A)
                    .unwrap_or_else(|error| panic!("test key must parse: {error}")),
                WEB_ADMIN_ACTOR,
            )
            .unwrap_or_else(|error| panic!("test client must be added: {error}"));

        let response = request(router(state), "GET", "/", String::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(CONTENT_SECURITY_POLICY));
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        let body = to_bytes(response.into_body(), 128 * 1024)
            .await
            .unwrap_or_else(|error| panic!("response body must be readable: {error}"));
        let body = std::str::from_utf8(&body)
            .unwrap_or_else(|error| panic!("response body must be UTF-8: {error}"));
        assert!(body.contains("fictional-node-a"));
        assert!(body.contains("Active"));
        assert!(body.contains(TEST_TOKEN));
    }

    #[tokio::test]
    async fn requires_csrf_and_applies_registry_mutations() {
        let root = TestDirectory::create();
        let state = test_state(root.path());
        let app = router(state.clone());

        let rejected = request(
            app.clone(),
            "POST",
            "/clients",
            encoded_form(&[
                ("csrf_token", "wrong-token"),
                ("client_id", "fictional-node-a"),
                ("public_key", KEY_A),
            ]),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
        assert!(
            state
                .registry
                .current()
                .unwrap_or_else(|error| panic!("test registry must remain readable: {error}"))
                .is_none()
        );

        let added = request(
            app.clone(),
            "POST",
            "/clients",
            encoded_form(&[
                ("csrf_token", TEST_TOKEN),
                ("client_id", "fictional-node-a"),
                ("public_key", KEY_A),
            ]),
        )
        .await;
        assert_eq!(added.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            added.headers().get(LOCATION),
            Some(&HeaderValue::from_static("/?notice=added"))
        );

        let disabled = request(
            app.clone(),
            "POST",
            "/clients/fictional-node-a/disable",
            encoded_form(&[("csrf_token", TEST_TOKEN)]),
        )
        .await;
        assert_eq!(disabled.status(), StatusCode::SEE_OTHER);
        let snapshot = state
            .registry
            .current()
            .unwrap_or_else(|error| panic!("test registry must be readable: {error}"))
            .unwrap_or_else(|| panic!("test registry must have a generation"));
        assert_eq!(snapshot.clients()[0].status(), ClientStatus::Disabled);

        let enabled = request(
            app.clone(),
            "POST",
            "/clients/fictional-node-a/enable",
            encoded_form(&[("csrf_token", TEST_TOKEN)]),
        )
        .await;
        assert_eq!(enabled.status(), StatusCode::SEE_OTHER);
        let fingerprint = state
            .registry
            .current()
            .unwrap_or_else(|error| panic!("test registry must be readable: {error}"))
            .unwrap_or_else(|| panic!("test registry must have a generation"))
            .clients()[0]
            .fingerprint()
            .to_owned();

        let rotated = request(
            app,
            "POST",
            "/clients/fictional-node-a/rotate-key",
            encoded_form(&[
                ("csrf_token", TEST_TOKEN),
                ("expected_fingerprint", &fingerprint),
                ("public_key", KEY_B),
            ]),
        )
        .await;
        assert_eq!(rotated.status(), StatusCode::SEE_OTHER);
        let snapshot = state
            .registry
            .current()
            .unwrap_or_else(|error| panic!("test registry must be readable: {error}"))
            .unwrap_or_else(|| panic!("test registry must have a generation"));
        assert_eq!(snapshot.clients()[0].status(), ClientStatus::Active);
        assert_eq!(
            snapshot.clients()[0].public_key(),
            NormalizedPublicKey::parse(KEY_B)
                .unwrap_or_else(|error| panic!("test key must parse: {error}"))
                .openssh()
        );
    }

    #[test]
    fn rejects_relative_socket_path() {
        let result = run(
            PathBuf::from("/srv/fictional-access"),
            PathBuf::from("fictional-admin.sock"),
        );
        assert!(matches!(
            result,
            Err(ClientAdminWebError::InvalidSocketPath(_))
        ));
    }

    #[test]
    fn creates_a_group_restricted_unix_socket_and_removes_it() {
        let root = TestDirectory::create();
        let socket_path = root.path().join("admin.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path)
            .unwrap_or_else(|error| panic!("test socket must bind: {error}"));
        let socket = BoundSocket::new(socket_path.clone())
            .unwrap_or_else(|error| panic!("bound socket must validate: {error}"));
        socket
            .set_permissions()
            .unwrap_or_else(|error| panic!("socket permissions must be set: {error}"));
        assert_eq!(
            fs::symlink_metadata(&socket_path)
                .unwrap_or_else(|error| panic!("socket metadata must be readable: {error}"))
                .permissions()
                .mode()
                & 0o777,
            0o660
        );
        drop(listener);
        drop(socket);
        assert!(!socket_path.exists());
    }

    #[test]
    fn refuses_a_live_socket_and_reclaims_it_after_the_listener_stops() {
        let root = TestDirectory::create();
        let socket_path = root.path().join("admin.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path)
            .unwrap_or_else(|error| panic!("test socket must bind: {error}"));

        assert!(matches!(
            clear_stale_socket(&socket_path),
            Err(ClientAdminWebError::SocketAlreadyExists(_))
        ));
        assert!(socket_path.exists());

        drop(listener);
        clear_stale_socket(&socket_path)
            .unwrap_or_else(|error| panic!("stale socket must be reclaimed: {error}"));
        assert!(!socket_path.exists());
    }
}
