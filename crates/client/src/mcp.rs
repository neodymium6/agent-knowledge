mod inspect;
use agent_knowledge_protocol::{InspectRequest, InspectResponse};
use inspect::{
    ContextParameters, DiffParameters, ExcerptParameters, GetAtParameters, HistoryParameters,
    ProjectsParameters,
};
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use agent_knowledge_core::{DocumentId, ProjectId, RequestId, SessionId};
use agent_knowledge_protocol::{
    GetRequest, GetResponse, ListRequest, ListResponse, ReadFilterRequest, SearchRequest,
    StatusRequest, StatusResponse, SubmitResponse,
};
use axum::{Router, http::StatusCode, routing::get};
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::mcp_archive::ArchiveDocumentParameters;
use crate::mcp_create::CreateDocumentParameters;
use crate::mcp_package::PreparedPackage;
use crate::{ClientCommandError, SshClient};

const DEFAULT_MAXIMUM_RESULTS: usize = 100;
const MAXIMUM_RESULTS: usize = 10_000;

trait KnowledgeBackend: Clone + Send + Sync + 'static {
    type Error: fmt::Display + Send + 'static;

    fn format_error(error: &Self::Error) -> String {
        error.to_string()
    }

    fn server_version(&self) -> crate::version::ServerVersion;
    fn submit(&self, package_root: &Path) -> Result<SubmitResponse, Self::Error>;
    fn list(&self, request: &ListRequest) -> Result<ListResponse, Self::Error>;
    fn recent(&self, request: &ListRequest) -> Result<ListResponse, Self::Error>;
    fn search(&self, request: &SearchRequest) -> Result<ListResponse, Self::Error>;
    fn get(&self, request: &GetRequest) -> Result<GetResponse, Self::Error>;
    fn status(&self, request: &StatusRequest) -> Result<StatusResponse, Self::Error>;
    fn inspect(&self, request: &InspectRequest) -> Result<InspectResponse, Self::Error>;
}

impl KnowledgeBackend for SshClient {
    type Error = ClientCommandError;

    fn server_version(&self) -> crate::version::ServerVersion {
        SshClient::server_version(self)
    }

    fn format_error(error: &Self::Error) -> String {
        error.mcp_message()
    }

    fn submit(&self, package_root: &Path) -> Result<SubmitResponse, Self::Error> {
        SshClient::submit(self, package_root)
    }

    fn list(&self, request: &ListRequest) -> Result<ListResponse, Self::Error> {
        SshClient::list(self, request)
    }

    fn recent(&self, request: &ListRequest) -> Result<ListResponse, Self::Error> {
        SshClient::recent(self, request)
    }

    fn search(&self, request: &SearchRequest) -> Result<ListResponse, Self::Error> {
        SshClient::search(self, request)
    }

    fn get(&self, request: &GetRequest) -> Result<GetResponse, Self::Error> {
        SshClient::get(self, request)
    }

    fn inspect(&self, request: &InspectRequest) -> Result<InspectResponse, Self::Error> {
        SshClient::inspect(self, request)
    }

    fn status(&self, request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
        SshClient::status(self, request)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadParameters {
    /// Restrict results to one project slug. Do not combine with projects.
    #[serde(default)]
    project: Option<String>,
    /// Restrict results to any of 1..32 distinct project slugs. Do not combine with project.
    #[serde(default)]
    projects: Option<Vec<String>>,
    /// Restrict results to one exact tag.
    #[serde(default)]
    tag: Option<String>,
    /// Restrict results to one coding-agent session ULID.
    #[serde(default)]
    session: Option<String>,
    /// Include documents located below archive directories.
    #[serde(default)]
    include_archived: bool,
    /// Maximum number of results, from 1 through 10000. Defaults to 100.
    #[serde(default)]
    #[schemars(range(min = 1, max = 10000))]
    maximum_results: Option<usize>,
}

impl ReadParameters {
    fn into_request(self) -> Result<ListRequest, String> {
        let maximum_results = self.maximum_results.unwrap_or(DEFAULT_MAXIMUM_RESULTS);
        if !(1..=MAXIMUM_RESULTS).contains(&maximum_results) {
            return Err(format!(
                "maximum_results must be between 1 and {MAXIMUM_RESULTS}"
            ));
        }
        let project = self
            .project
            .map(|value| {
                value
                    .parse::<ProjectId>()
                    .map_err(|_| "project must be a valid project slug".to_owned())
            })
            .transpose()?;
        let projects = self
            .projects
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        value
                            .parse::<ProjectId>()
                            .map_err(|_| "projects must contain valid project slugs".to_owned())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let session = self
            .session
            .map(|value| {
                value
                    .parse::<SessionId>()
                    .map_err(|_| "session must be a canonical ULID".to_owned())
            })
            .transpose()?;
        if self.tag.as_deref().is_some_and(str::is_empty) {
            return Err("tag must not be empty".to_owned());
        }
        let filter = ReadFilterRequest {
            project,
            projects,
            tag: self.tag,
            session,
            include_archived: self.include_archived,
        };
        if !filter.valid_project_selection() {
            return Err(
                "projects must contain 1..32 distinct slugs and cannot be combined with project"
                    .into(),
            );
        }
        Ok(ListRequest::new(filter, maximum_results))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchParameters {
    /// Search expression using the configured backend: Tantivy query syntax when indexed,
    /// or case-insensitive substring matching when indexing is disabled.
    query: String,
    /// Restrict results to one project slug. Do not combine with projects.
    #[serde(default)]
    project: Option<String>,
    /// Restrict results to any of 1..32 distinct project slugs. Do not combine with project.
    #[serde(default)]
    projects: Option<Vec<String>>,
    /// Restrict results to one exact tag.
    #[serde(default)]
    tag: Option<String>,
    /// Restrict results to one coding-agent session ULID.
    #[serde(default)]
    session: Option<String>,
    /// Include documents located below archive directories.
    #[serde(default)]
    include_archived: bool,
    /// Maximum number of results, from 1 through 10000. Defaults to 100.
    #[serde(default)]
    #[schemars(range(min = 1, max = 10000))]
    maximum_results: Option<usize>,
}

impl SearchParameters {
    fn into_request(self) -> Result<SearchRequest, String> {
        if self.query.trim().is_empty() {
            return Err("query must not be empty".to_owned());
        }
        let query = self.query;
        let list = ReadParameters {
            project: self.project,
            projects: self.projects,
            tag: self.tag,
            session: self.session,
            include_archived: self.include_archived,
            maximum_results: self.maximum_results,
        }
        .into_request()?;
        Ok(SearchRequest::new(query, list.filter, list.maximum_results))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DocumentParameters {
    /// Permanent document ULID. It remains valid when the document moves.
    document_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VersionParameters {
    /// Check upstream if the shared 24-hour cache is due. Defaults to false (cache only).
    /// AGENT_KNOWLEDGE_UPDATE_CHECK=off overrides this option.
    #[serde(default)]
    check_updates: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StatusParameters {
    /// Permanent request ULID from package submission, structured creation, or archival.
    request_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubmitParameters {
    /// Directory on the MCP server's filesystem containing request.json and payload/.
    package_root: String,
}

#[derive(Clone, Debug)]
struct KnowledgeMcpServer<C: KnowledgeBackend> {
    client: C,
    tool_router: ToolRouter<Self>,
}

impl<C: KnowledgeBackend> KnowledgeMcpServer<C> {
    fn new(client: C) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl<C: KnowledgeBackend> KnowledgeMcpServer<C> {
    #[tool(
        name = "knowledge_version",
        description = "Read client/Gateway versions, protocol capabilities, and stable release/cache status. Optional upstream checks use the shared 24-hour cache and honor disabled checks. Release differences do not imply incompatibility.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn version(
        &self,
        Parameters(parameters): Parameters<VersionParameters>,
    ) -> Result<CallToolResult, String> {
        let client = self.client.clone();
        let report = run_blocking(
            move || {
                Ok::<_, C::Error>(crate::version::VersionReport::new(
                    client.server_version(),
                    parameters.check_updates,
                ))
            },
            C::format_error,
        )
        .await?;
        structured(report)
    }

    #[tool(
        name = "knowledge_projects",
        description = "Discover projects by slug/index text, or search their documents and rank projects by matching document count. Returns bounded descriptions and document counts; includes projects without an index. Optional hits_per_project (1..5) includes ranked matching documents and excerpts in documents mode.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn projects(
        &self,
        Parameters(parameters): Parameters<ProjectsParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_search_excerpts",
        description = "Search committed knowledge and return bounded raw query-term excerpts with field names.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn search_excerpts(
        &self,
        Parameters(parameters): Parameters<ExcerptParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_context",
        description = "Read a project index, relevant guidance and recent records from one commit within a body character budget. Returns selection reasons and truncation flags. Balanced selection reserves recent logs and shares the body budget.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn context(
        &self,
        Parameters(parameters): Parameters<ContextParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_history",
        description = "List document Markdown and path changes in official commit history, following permanent identity across moves. Use anchor_commit and next_cursor for stable pagination.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn history(
        &self,
        Parameters(parameters): Parameters<HistoryParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_get_at",
        description = "Read exact Markdown for a document at an official historical commit.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn get_at(
        &self,
        Parameters(parameters): Parameters<GetAtParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_diff",
        description = "Compare one document at two official commits. Returns before/after metadata and a contiguous body range by default. Use format hunks for bounded separate ranges with explicit truncation.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn diff(
        &self,
        Parameters(parameters): Parameters<DiffParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.inspect(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_archive_document",
        description = "Archive one active mutable Agent Knowledge document without a caller-visible request package. Reuse document_id, expected_revision, request_id, and created_at together to retry an uncertain response.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn archive_document(
        &self,
        Parameters(parameters): Parameters<ArchiveDocumentParameters>,
    ) -> Result<CallToolResult, String> {
        let intent = parameters.parse()?;
        let get_request = intent.get_request();
        let client = self.client.clone();
        let document = run_blocking(move || client.get(&get_request), C::format_error).await?;
        let package = intent.prepare(&document.document.summary)?;
        structured(submit_prepared(self.client.clone(), package).await?)
    }

    #[tool(
        name = "knowledge_create_document",
        description = "Create and submit one Agent Knowledge Markdown document without a caller-visible request package. Reuse request_id, document_id, and created_at together to retry an uncertain response.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn create_document(
        &self,
        Parameters(parameters): Parameters<CreateDocumentParameters>,
    ) -> Result<CallToolResult, String> {
        let package = parameters.prepare()?;
        structured(submit_prepared(self.client.clone(), package).await?)
    }

    #[tool(
        name = "knowledge_list",
        description = "List committed Agent Knowledge documents in canonical path order.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn list(
        &self,
        Parameters(parameters): Parameters<ReadParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.list(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_recent",
        description = "List committed Agent Knowledge documents by recorded updated time (or created time when absent), newest first.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn recent(
        &self,
        Parameters(parameters): Parameters<ReadParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.recent(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_search",
        description = "Search committed Agent Knowledge Markdown and permitted metadata.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn search(
        &self,
        Parameters(parameters): Parameters<SearchParameters>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(run_blocking(move || client.search(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_get",
        description = "Get one committed Markdown document by permanent document ID.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn get(
        &self,
        Parameters(parameters): Parameters<DocumentParameters>,
    ) -> Result<CallToolResult, String> {
        let document_id = parameters
            .document_id
            .parse::<DocumentId>()
            .map_err(|_| "document_id must be a canonical ULID".to_owned())?;
        let request = GetRequest::new(document_id);
        let client = self.client.clone();
        structured(run_blocking(move || client.get(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_request_status",
        description = "Get the durable queue state of an accepted Agent Knowledge request.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn request_status(
        &self,
        Parameters(parameters): Parameters<StatusParameters>,
    ) -> Result<CallToolResult, String> {
        let request_id = parameters
            .request_id
            .parse::<RequestId>()
            .map_err(|_| "request_id must be a canonical ULID".to_owned())?;
        let request = StatusRequest::new(request_id);
        let client = self.client.clone();
        structured(run_blocking(move || client.status(&request), C::format_error).await?)
    }

    #[tool(
        name = "knowledge_submit_package",
        description = "Validate and submit one local immutable Agent Knowledge request package over SSH.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn submit_package(
        &self,
        Parameters(parameters): Parameters<SubmitParameters>,
    ) -> Result<CallToolResult, String> {
        if parameters.package_root.is_empty() {
            return Err("package_root must not be empty".to_owned());
        }
        let package_root = parameters.package_root;
        let client = self.client.clone();
        structured(
            run_blocking(
                move || client.submit(Path::new(&package_root)),
                C::format_error,
            )
            .await?,
        )
    }
}

#[tool_handler(router = self.tool_router)]
impl<C: KnowledgeBackend> ServerHandler for KnowledgeMcpServer<C> {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "agent-knowledge-client",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Use knowledge_projects for project discovery; knowledge_list, knowledge_recent, knowledge_search, knowledge_search_excerpts, knowledge_context, and knowledge_get for committed reads; knowledge_history, knowledge_get_at, and knowledge_diff for official history. knowledge_version reports client/Gateway versions and Gateway capabilities. knowledge_create_document and knowledge_archive_document accept structured writes; knowledge_submit_package accepts a complete package directory on this MCP server's filesystem. knowledge_request_status tracks accepted requests. The SSH destination and credentials are configured on this machine.",
            )
    }
}

enum StructuredSubmitError<E> {
    Local,
    Backend(E),
}

async fn submit_prepared<C: KnowledgeBackend>(
    client: C,
    package: PreparedPackage,
) -> Result<SubmitResponse, String> {
    tokio::task::spawn_blocking(move || {
        let temporary = package
            .materialize()
            .map_err(|_| StructuredSubmitError::Local)?;
        client
            .submit(temporary.path())
            .map_err(StructuredSubmitError::Backend)
    })
    .await
    .map_err(|_| "local Agent Knowledge operation stopped unexpectedly".to_owned())?
    .map_err(|error| match error {
        StructuredSubmitError::Local => {
            "could not create a private temporary request package".to_owned()
        }
        StructuredSubmitError::Backend(error) => C::format_error(&error),
    })
}

async fn run_blocking<T, E>(
    operation: impl FnOnce() -> Result<T, E> + Send + 'static,
    format_error: fn(&E) -> String,
) -> Result<T, String>
where
    T: Send + 'static,
    E: fmt::Display + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| "local Agent Knowledge operation stopped unexpectedly".to_owned())?
        .map_err(|error| format_error(&error))
}

fn structured(value: impl Serialize) -> Result<CallToolResult, String> {
    serde_json::to_value(value)
        .map(CallToolResult::structured)
        .map_err(|_| "could not encode the Agent Knowledge response".to_owned())
}

pub(crate) fn run(client: SshClient, listen: Option<SocketAddr>) -> Result<(), McpServerError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(McpServerError::Runtime)?
        .block_on(async move {
            if let Some(address) = listen {
                return run_http(client, address).await;
            }
            let service = KnowledgeMcpServer::new(client)
                .serve(rmcp::transport::stdio())
                .await
                .map_err(|error| McpServerError::Initialize(Box::new(error)))?;
            service.waiting().await.map_err(McpServerError::Service)?;
            Ok(())
        })
}

async fn run_http(client: SshClient, address: SocketAddr) -> Result<(), McpServerError> {
    let config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_allowed_hosts([address.to_string(), format!("localhost:{}", address.port())]);
    let cancellation = config.cancellation_token.clone();
    let router = http_router(client, config);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|source| McpServerError::Bind { address, source })?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            wait_for_shutdown().await;
            cancellation.cancel();
        })
        .await
        .map_err(McpServerError::Serve)
}

fn http_router<C: KnowledgeBackend>(client: C, config: StreamableHttpServerConfig) -> Router {
    let service: StreamableHttpService<KnowledgeMcpServer<C>, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(KnowledgeMcpServer::new(client.clone())),
            Default::default(),
            config,
        );
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .nest_service("/mcp", service)
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
pub enum McpServerError {
    Runtime(std::io::Error),
    Bind {
        address: SocketAddr,
        source: std::io::Error,
    },
    Serve(std::io::Error),
    Initialize(Box<rmcp::service::ServerInitializeError>),
    Service(tokio::task::JoinError),
}

impl fmt::Display for McpServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(formatter, "could not start the MCP runtime: {error}"),
            Self::Bind { address, source } => {
                write!(
                    formatter,
                    "could not bind MCP listener at {address}: {source}"
                )
            }
            Self::Serve(error) => write!(formatter, "MCP HTTP server failed: {error}"),
            Self::Initialize(error) => write!(formatter, "could not initialize MCP: {error}"),
            Self::Service(error) => write!(formatter, "MCP service failed: {error}"),
        }
    }
}

impl std::error::Error for McpServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Bind { source, .. } => Some(source),
            Self::Serve(error) => Some(error),
            Self::Initialize(error) => Some(error),
            Self::Service(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use agent_knowledge_protocol::{InspectRequest, InspectResponse};
    use std::convert::Infallible;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::{
        DocumentParameters, KnowledgeBackend, KnowledgeMcpServer, Parameters, ReadParameters,
        http_router,
    };
    use crate::mcp_archive::ArchiveDocumentParameters;
    use crate::mcp_create::CreateDocumentParameters;
    use agent_knowledge_core::{ChangeRequest, Operation};
    use agent_knowledge_protocol::{
        GetRequest, GetResponse, ListRequest, ListResponse, SearchRequest, StatusRequest,
        StatusResponse, SubmitOutcome, SubmitResponse,
    };
    use rmcp::{
        ServiceExt,
        model::{CallToolRequestParams, ClientInfo},
        transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
            streamable_http_server::StreamableHttpServerConfig,
        },
    };

    #[test]
    fn project_filters_reject_ambiguous_or_empty_scopes_and_preserve_legacy_wire() {
        for value in [
            serde_json::json!({"query":"needle", "projects":[]}),
            serde_json::json!({"query":"needle", "project":"fictional-a", "projects":["fictional-b"]}),
            serde_json::json!({"query":"needle", "projects":["fictional-a","fictional-a"]}),
            serde_json::json!({"query":"needle", "projects":["../invalid"]}),
        ] {
            let params: super::SearchParameters =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("parameters: {e}"));
            assert!(params.into_request().is_err());
        }
        let params: super::SearchParameters =
            serde_json::from_value(serde_json::json!({"query":"needle","project":"fictional-a"}))
                .unwrap_or_else(|e| panic!("parameters: {e}"));
        let value = serde_json::to_value(
            params
                .into_request()
                .unwrap_or_else(|e| panic!("request: {e}")),
        )
        .unwrap_or_else(|e| panic!("JSON: {e}"));
        assert_eq!(
            value,
            serde_json::json!({"protocol_version":1,"query":"needle","project":"fictional-a","maximum_results":100})
        );
    }

    fn local_http_client() -> reqwest::Client {
        // HTTP fixtures do not depend on platform roots or global provider initialization.
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|e| panic!("TLS fixture: {e}"))
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        reqwest::Client::builder()
            .no_proxy()
            .tls_backend_preconfigured(tls)
            .build()
            .unwrap_or_else(|e| panic!("HTTP fixture: {e}"))
    }

    #[derive(Clone, Debug)]
    struct FakeBackend;

    impl KnowledgeBackend for FakeBackend {
        type Error = Infallible;
        fn server_version(&self) -> crate::version::ServerVersion {
            crate::version::ServerVersion::Unsupported
        }

        fn submit(&self, _package_root: &std::path::Path) -> Result<SubmitResponse, Self::Error> {
            unreachable!()
        }

        fn list(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            Ok(ListResponse::new("fictional-commit".to_owned(), Vec::new()))
        }

        fn recent(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn search(&self, _request: &SearchRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn get(&self, _request: &GetRequest) -> Result<GetResponse, Self::Error> {
            unreachable!()
        }

        fn inspect(&self, request: &InspectRequest) -> Result<InspectResponse, Self::Error> {
            use agent_knowledge_protocol::{BodyDiff, BodyHunks, InspectQuery, Inspection};
            let result = match &request.query {
                InspectQuery::Projects {
                    search_in, query, ..
                } => {
                    assert_eq!(
                        *search_in,
                        agent_knowledge_protocol::ProjectSearchScope::Documents
                    );
                    assert_eq!(query.as_deref(), Some("needle"));
                    Inspection::Projects {
                        commit: "a".repeat(40),
                        projects: vec![],
                        truncated: false,
                    }
                }
                InspectQuery::ProjectsWithHits {
                    query,
                    hits_per_project,
                    excerpt_characters,
                    ..
                } => {
                    assert_eq!(query, "needle");
                    assert_eq!(*hits_per_project, 3);
                    assert_eq!(*excerpt_characters, 300);
                    Inspection::Projects {
                        commit: "a".repeat(40),
                        projects: vec![],
                        truncated: false,
                    }
                }
                InspectQuery::ContextBalanced {
                    recent_documents,
                    maximum_documents,
                    ..
                } => {
                    assert_eq!(*recent_documents, 1);
                    assert_eq!(*maximum_documents, 3);
                    Inspection::Context {
                        commit: "a".repeat(40),
                        documents: vec![],
                        additional: vec![],
                        truncated: false,
                    }
                }
                InspectQuery::DiffHunks {
                    from_commit,
                    to_commit,
                    context_lines,
                    ..
                } => {
                    assert_eq!(*context_lines, 0);
                    let document = archive_response().document.summary;
                    Inspection::DiffHunks {
                        from_commit: from_commit.clone(),
                        to_commit: to_commit.clone(),
                        before: Box::new(document.clone()),
                        after: Box::new(document),
                        body: BodyHunks {
                            changed: true,
                            hunks: vec![BodyDiff {
                                from_line: 1,
                                to_line: 1,
                                removed: "old\n".into(),
                                added: "new\n".into(),
                            }],
                            truncated: false,
                            truncation_reason: None,
                        },
                    }
                }
                _ => panic!("unexpected inspection"),
            };
            Ok(InspectResponse {
                protocol_version: 1,
                result,
            })
        }

        fn status(&self, _request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
            unreachable!()
        }
    }

    #[derive(Clone, Debug)]
    struct TestSubmitBackend {
        submitted_path: Arc<Mutex<Option<PathBuf>>>,
        fail: bool,
    }

    impl KnowledgeBackend for TestSubmitBackend {
        type Error = &'static str;
        fn server_version(&self) -> crate::version::ServerVersion {
            crate::version::ServerVersion::Unavailable
        }

        fn submit(&self, package_root: &Path) -> Result<SubmitResponse, Self::Error> {
            let package = agent_knowledge_queue::validate_package(
                package_root,
                &agent_knowledge_queue::PackagePolicy::default(),
            )
            .map_err(|_| "fictional package validation failed")?;
            let mut submitted_path = self
                .submitted_path
                .lock()
                .map_err(|_| "fictional test lock failed")?;
            *submitted_path = Some(package_root.to_owned());
            if self.fail {
                return Err("fictional submit failed");
            }
            Ok(SubmitResponse::new(SubmitOutcome::Accepted {
                request_id: package.request().request_id,
                digest: package.digest().as_revision(),
            }))
        }

        fn list(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn recent(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn search(&self, _request: &SearchRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn get(&self, _request: &GetRequest) -> Result<GetResponse, Self::Error> {
            unreachable!()
        }

        fn inspect(&self, _request: &InspectRequest) -> Result<InspectResponse, Self::Error> {
            unreachable!()
        }

        fn status(&self, _request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
            unreachable!()
        }
    }

    #[derive(Clone, Debug)]
    struct ArchiveBackend {
        document: Option<GetResponse>,
        submitted_path: Arc<Mutex<Option<PathBuf>>>,
        submitted_request: Arc<Mutex<Option<ChangeRequest>>>,
        fail_submit: bool,
    }

    impl KnowledgeBackend for ArchiveBackend {
        type Error = &'static str;
        fn server_version(&self) -> crate::version::ServerVersion {
            crate::version::ServerVersion::Unavailable
        }

        fn submit(&self, package_root: &Path) -> Result<SubmitResponse, Self::Error> {
            let package = agent_knowledge_queue::validate_package(
                package_root,
                &agent_knowledge_queue::PackagePolicy::default(),
            )
            .map_err(|_| "fictional package validation failed")?;
            *self
                .submitted_path
                .lock()
                .map_err(|_| "fictional path lock failed")? = Some(package_root.to_owned());
            *self
                .submitted_request
                .lock()
                .map_err(|_| "fictional request lock failed")? = Some(package.request().clone());
            if self.fail_submit {
                return Err("fictional archive submit failed");
            }
            Ok(SubmitResponse::new(SubmitOutcome::Accepted {
                request_id: package.request().request_id,
                digest: package.digest().as_revision(),
            }))
        }

        fn list(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn recent(&self, _request: &ListRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn search(&self, _request: &SearchRequest) -> Result<ListResponse, Self::Error> {
            unreachable!()
        }

        fn get(&self, _request: &GetRequest) -> Result<GetResponse, Self::Error> {
            self.document.clone().ok_or("fictional document not found")
        }

        fn inspect(&self, _request: &InspectRequest) -> Result<InspectResponse, Self::Error> {
            unreachable!()
        }

        fn status(&self, _request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
            unreachable!()
        }
    }

    #[test]
    fn advertises_the_expected_tool_set() {
        let server = KnowledgeMcpServer::new(FakeBackend);
        let tools = server.tool_router.list_all();
        let submit = tools
            .iter()
            .find(|tool| tool.name == "knowledge_submit_package")
            .unwrap_or_else(|| panic!("submit tool must be advertised"));
        assert_eq!(
            submit
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.idempotent_hint),
            Some(false)
        );
        assert_eq!(
            submit
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.destructive_hint),
            Some(true)
        );
        for name in ["knowledge_list", "knowledge_search"] {
            let tool = tools
                .iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("{name} tool must be advertised"));
            let schema = serde_json::to_value(&tool.input_schema)
                .unwrap_or_else(|error| panic!("{name} schema must encode: {error}"));
            assert_eq!(
                schema["properties"]["maximum_results"]["minimum"],
                serde_json::json!(1)
            );
            assert_eq!(
                schema["properties"]["maximum_results"]["maximum"],
                serde_json::json!(10000)
            );
        }
        let names = tools
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "knowledge_archive_document",
                "knowledge_context",
                "knowledge_create_document",
                "knowledge_diff",
                "knowledge_get",
                "knowledge_get_at",
                "knowledge_history",
                "knowledge_list",
                "knowledge_projects",
                "knowledge_recent",
                "knowledge_request_status",
                "knowledge_search",
                "knowledge_search_excerpts",
                "knowledge_submit_package",
                "knowledge_version",
            ]
        );

        let create = server
            .tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "knowledge_create_document")
            .unwrap_or_else(|| panic!("create tool must be advertised"));
        assert_eq!(
            create
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.destructive_hint),
            Some(false)
        );
        let schema = serde_json::to_value(&create.input_schema)
            .unwrap_or_else(|error| panic!("create schema must encode: {error}"));
        assert!(schema["properties"].get("body").is_some());
        assert!(schema["properties"].get("package_root").is_none());

        let archive = server
            .tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "knowledge_archive_document")
            .unwrap_or_else(|| panic!("archive tool must be advertised"));
        assert_eq!(
            archive
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.read_only_hint),
            Some(false)
        );
        assert_eq!(
            archive
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.destructive_hint),
            Some(true)
        );
        assert_eq!(
            archive
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.idempotent_hint),
            Some(false)
        );
        assert_eq!(
            archive
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.open_world_hint),
            Some(true)
        );
        let schema = serde_json::to_value(&archive.input_schema)
            .unwrap_or_else(|error| panic!("archive schema must encode: {error}"));
        assert!(schema["properties"].get("document_id").is_some());
        assert!(schema["properties"].get("expected_revision").is_some());
        assert!(schema["properties"].get("package_root").is_none());
    }

    #[tokio::test]
    async fn removes_the_structured_package_after_submit_failure() {
        let submitted_path = Arc::new(Mutex::new(None));
        let backend = TestSubmitBackend {
            submitted_path: submitted_path.clone(),
            fail: true,
        };
        let parameters = create_parameters();

        let result = KnowledgeMcpServer::new(backend)
            .create_document(Parameters(parameters))
            .await;
        assert_eq!(result.err().as_deref(), Some("fictional submit failed"));
        assert_submitted_package_removed(&submitted_path);
    }

    #[tokio::test]
    async fn submits_a_structured_document_and_removes_its_package() {
        let submitted_path = Arc::new(Mutex::new(None));
        let backend = TestSubmitBackend {
            submitted_path: submitted_path.clone(),
            fail: false,
        };

        let result = KnowledgeMcpServer::new(backend)
            .create_document(Parameters(create_parameters()))
            .await
            .unwrap_or_else(|error| panic!("create tool must succeed: {error}"));
        assert_eq!(
            result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("request_id"))
                .and_then(serde_json::Value::as_str),
            Some("01K00000000000000000000003")
        );
        assert_submitted_package_removed(&submitted_path);
    }

    #[tokio::test]
    async fn archives_a_document_and_removes_its_package() {
        let submitted_path = Arc::new(Mutex::new(None));
        let submitted_request = Arc::new(Mutex::new(None));
        let backend = ArchiveBackend {
            document: Some(archive_response()),
            submitted_path: submitted_path.clone(),
            submitted_request: submitted_request.clone(),
            fail_submit: false,
        };

        let result = KnowledgeMcpServer::new(backend)
            .archive_document(Parameters(archive_parameters()))
            .await
            .unwrap_or_else(|error| panic!("archive tool must succeed: {error}"));
        assert_eq!(
            result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("request_id"))
                .and_then(serde_json::Value::as_str),
            Some("01K00000000000000000000005")
        );
        let request = submitted_request
            .lock()
            .unwrap_or_else(|error| panic!("submitted request lock must succeed: {error}"))
            .clone()
            .unwrap_or_else(|| panic!("backend must observe the request"));
        assert_eq!(request.title, "Archive document 01K00000000000000000000004");
        assert!(matches!(
            request.operations.as_slice(),
            [Operation::ArchiveDocument { .. }]
        ));
        assert_submitted_package_removed(&submitted_path);
    }

    #[tokio::test]
    async fn removes_the_archive_package_after_submit_failure() {
        let submitted_path = Arc::new(Mutex::new(None));
        let backend = ArchiveBackend {
            document: Some(archive_response()),
            submitted_path: submitted_path.clone(),
            submitted_request: Arc::new(Mutex::new(None)),
            fail_submit: true,
        };

        let result = KnowledgeMcpServer::new(backend)
            .archive_document(Parameters(archive_parameters()))
            .await;
        assert_eq!(
            result.err().as_deref(),
            Some("fictional archive submit failed")
        );
        assert_submitted_package_removed(&submitted_path);
    }

    #[tokio::test]
    async fn returns_missing_document_without_submitting_archive() {
        let submitted_path = Arc::new(Mutex::new(None));
        let backend = ArchiveBackend {
            document: None,
            submitted_path: submitted_path.clone(),
            submitted_request: Arc::new(Mutex::new(None)),
            fail_submit: false,
        };

        let result = KnowledgeMcpServer::new(backend)
            .archive_document(Parameters(archive_parameters()))
            .await;
        assert_eq!(
            result.err().as_deref(),
            Some("fictional document not found")
        );
        assert!(
            submitted_path
                .lock()
                .unwrap_or_else(|error| panic!("submitted path lock must succeed: {error}"))
                .is_none()
        );
    }

    fn create_parameters() -> CreateDocumentParameters {
        serde_json::from_value::<CreateDocumentParameters>(serde_json::json!({
            "title": "Record fictional result",
            "body": "The fictional result is reproducible.",
            "project": "fictional-solver",
            "document_type": "experiment",
            "request_id": "01K00000000000000000000003",
            "document_id": "01K00000000000000000000004",
            "created_at": "2026-08-05T10:00:00Z"
        }))
        .unwrap_or_else(|error| panic!("create parameters must decode: {error}"))
    }

    fn archive_parameters() -> ArchiveDocumentParameters {
        serde_json::from_value::<ArchiveDocumentParameters>(archive_parameter_value())
            .unwrap_or_else(|error| panic!("archive parameters must decode: {error}"))
    }

    fn archive_parameter_value() -> serde_json::Value {
        serde_json::json!({
            "document_id": "01K00000000000000000000004",
            "expected_revision": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "request_id": "01K00000000000000000000005",
            "created_at": "2026-08-06T10:00:00Z"
        })
    }

    fn archive_response() -> GetResponse {
        serde_json::from_value(serde_json::json!({
            "protocol_version": 1,
            "commit": "fictional-commit",
            "document": {
                "summary": {
                    "path": "projects/fictional-solver/experiments/2026/08/fictional-result.md",
                    "document_type": "experiment",
                    "project": "fictional-solver",
                    "archived": false,
                    "revision": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "metadata": {
                        "schema_version": 1,
                        "document_id": "01K00000000000000000000004",
                        "title": "Fictional result",
                        "created": "2026-08-05T10:00:00Z",
                        "request_id": "01K00000000000000000000006",
                        "status": "active"
                    }
                },
                "markdown": "---\nschema_version: 1\n---\n\nFictional result."
            }
        }))
        .unwrap_or_else(|error| panic!("get response must decode: {error}"))
    }

    fn assert_submitted_package_removed(submitted_path: &Arc<Mutex<Option<PathBuf>>>) {
        let path = submitted_path
            .lock()
            .unwrap_or_else(|error| panic!("submitted path lock must succeed: {error}"))
            .clone()
            .unwrap_or_else(|| panic!("backend must observe the package path"));
        assert!(!path.exists());
    }

    #[test]
    fn returns_structured_read_results() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap_or_else(|error| panic!("test runtime must start: {error}"));
        let result = runtime
            .block_on(
                KnowledgeMcpServer::new(FakeBackend).list(Parameters(ReadParameters {
                    project: None,
                    projects: None,
                    tag: None,
                    session: None,
                    include_archived: false,
                    maximum_results: None,
                })),
            )
            .unwrap_or_else(|error| panic!("list tool must succeed: {error}"));
        assert_eq!(
            result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("commit"))
                .and_then(serde_json::Value::as_str),
            Some("fictional-commit")
        );
        assert_eq!(result.is_error, Some(false));
    }

    #[test]
    fn rejects_invalid_identifiers_before_ssh() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap_or_else(|error| panic!("test runtime must start: {error}"));
        let result = runtime.block_on(KnowledgeMcpServer::new(FakeBackend).get(Parameters(
            DocumentParameters {
                document_id: "not-a-ulid".to_owned(),
            },
        )));
        assert_eq!(
            result.err().as_deref(),
            Some("document_id must be a canonical ULID")
        );
    }

    #[tokio::test]
    async fn serves_a_read_tool_over_streamable_http() {
        let config = StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_sse_keep_alive(None);
        let cancellation = config.cancellation_token.clone();
        let router = http_router(FakeBackend, config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("test listener must bind: {error}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("test listener address must be available: {error}"));
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });

        let transport = StreamableHttpClientTransport::with_client(
            local_http_client(),
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        );
        let client = ClientInfo::default()
            .serve(transport)
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP client must initialize: {error}"));
        let result = client
            .call_tool(
                CallToolRequestParams::new("knowledge_list").with_arguments(serde_json::Map::new()),
            )
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP read tool must succeed: {error}"));

        assert_eq!(
            result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("commit"))
                .and_then(serde_json::Value::as_str),
            Some("fictional-commit")
        );
        for (tool, arguments, operation) in [
            (
                "knowledge_projects",
                serde_json::json!({"query":"needle","search_in":"documents","hits_per_project":3}),
                "projects",
            ),
            (
                "knowledge_projects",
                serde_json::json!({"query":"needle","search_in":"documents"}),
                "projects",
            ),
            (
                "knowledge_context",
                serde_json::json!({"project":"fictional-project","selection":"balanced","maximum_documents":3,"recent_documents":1}),
                "context",
            ),
            (
                "knowledge_diff",
                serde_json::json!({"document_id":"01K00000000000000000000004","from_commit":"a".repeat(40),"to_commit":"b".repeat(40),"format":"hunks","context_lines":0}),
                "diff_hunks",
            ),
        ] {
            let response = client
                .call_tool(
                    CallToolRequestParams::new(tool).with_arguments(
                        arguments
                            .as_object()
                            .cloned()
                            .unwrap_or_else(|| panic!("arguments")),
                    ),
                )
                .await
                .unwrap_or_else(|e| panic!("inspection over HTTP: {e}"));
            assert_ne!(response.is_error, Some(true));
            let value = response
                .structured_content
                .unwrap_or_else(|| panic!("structured inspection"));
            assert_eq!(value["result"]["operation"], operation);
            if operation == "diff_hunks" {
                assert_eq!(value["result"]["body"]["hunks"][0]["added"], "new\n");
            }
        }
        let version = client
            .call_tool(
                CallToolRequestParams::new("knowledge_version")
                    .with_arguments(serde_json::Map::new()),
            )
            .await
            .unwrap_or_else(|e| panic!("version tool: {e}"));
        let version = version
            .structured_content
            .unwrap_or_else(|| panic!("structured version"));
        assert_eq!(version["client_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(version["server"]["status"], "unsupported");
        client
            .cancel()
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP client must stop: {error}"));
        cancellation.cancel();
        server
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP server task must stop: {error}"))
            .unwrap_or_else(|error| panic!("HTTP MCP server must stop cleanly: {error}"));
    }

    #[tokio::test]
    async fn serves_the_archive_tool_over_streamable_http() {
        let submitted_path = Arc::new(Mutex::new(None));
        let backend = ArchiveBackend {
            document: Some(archive_response()),
            submitted_path: submitted_path.clone(),
            submitted_request: Arc::new(Mutex::new(None)),
            fail_submit: false,
        };
        let config = StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_sse_keep_alive(None);
        let cancellation = config.cancellation_token.clone();
        let router = http_router(backend, config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("test listener must bind: {error}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("test listener address must be available: {error}"));
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });

        let transport = StreamableHttpClientTransport::with_client(
            local_http_client(),
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        );
        let client = ClientInfo::default()
            .serve(transport)
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP client must initialize: {error}"));
        let arguments = archive_parameter_value()
            .as_object()
            .cloned()
            .unwrap_or_else(|| panic!("archive parameters must encode as an object"));
        let result = client
            .call_tool(
                CallToolRequestParams::new("knowledge_archive_document").with_arguments(arguments),
            )
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP archive tool must succeed: {error}"));

        assert_eq!(
            result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("request_id"))
                .and_then(serde_json::Value::as_str),
            Some("01K00000000000000000000005")
        );
        assert_submitted_package_removed(&submitted_path);
        client
            .cancel()
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP client must stop: {error}"));
        cancellation.cancel();
        server
            .await
            .unwrap_or_else(|error| panic!("HTTP MCP server task must stop: {error}"))
            .unwrap_or_else(|error| panic!("HTTP MCP server must stop cleanly: {error}"));
    }
}
