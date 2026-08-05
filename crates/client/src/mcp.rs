use std::fmt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use agent_knowledge_core::{DocumentId, ProjectId, RequestId, SessionId};
use agent_knowledge_protocol::{
    GetRequest, GetResponse, ListRequest, ListResponse, ReadFilterRequest, SearchRequest,
    StatusRequest, StatusResponse, SubmitResponse,
};
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo},
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{CancellationFlag, ClientCommandError, SshClient};

const DEFAULT_MAXIMUM_RESULTS: usize = 100;
const MAXIMUM_RESULTS: usize = 10_000;
const MAXIMUM_CONCURRENT_OPERATIONS: usize = 4;

trait KnowledgeBackend: Clone + Send + Sync + 'static {
    type Error: fmt::Display + Send + 'static;

    fn with_cancellation(self, cancellation: CancellationFlag) -> Self;
    fn submit(&self, package_root: &Path) -> Result<SubmitResponse, Self::Error>;
    fn list(&self, request: &ListRequest) -> Result<ListResponse, Self::Error>;
    fn recent(&self, request: &ListRequest) -> Result<ListResponse, Self::Error>;
    fn search(&self, request: &SearchRequest) -> Result<ListResponse, Self::Error>;
    fn get(&self, request: &GetRequest) -> Result<GetResponse, Self::Error>;
    fn status(&self, request: &StatusRequest) -> Result<StatusResponse, Self::Error>;
}

impl KnowledgeBackend for SshClient {
    type Error = ClientCommandError;

    fn with_cancellation(self, cancellation: CancellationFlag) -> Self {
        SshClient::with_cancellation(self, cancellation)
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

    fn status(&self, request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
        SshClient::status(self, request)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadParameters {
    /// Restrict results to one configured project slug.
    #[serde(default)]
    project: Option<String>,
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
        Ok(ListRequest::new(
            ReadFilterRequest {
                project,
                tag: self.tag,
                session,
                include_archived: self.include_archived,
            },
            maximum_results,
        ))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchParameters {
    /// Case-insensitive text to find in committed Markdown and permitted metadata.
    query: String,
    /// Restrict results to one configured project slug.
    #[serde(default)]
    project: Option<String>,
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
struct StatusParameters {
    /// Permanent request ULID returned by knowledge_submit_package.
    request_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubmitParameters {
    /// Local directory containing request.json and payload/ for one change request.
    package_root: String,
}

fn get_request(document_id: String) -> Result<GetRequest, String> {
    document_id
        .parse::<DocumentId>()
        .map(GetRequest::new)
        .map_err(|_| "document_id must be a canonical ULID".to_owned())
}

fn status_request(request_id: String) -> Result<StatusRequest, String> {
    request_id
        .parse::<RequestId>()
        .map(StatusRequest::new)
        .map_err(|_| "request_id must be a canonical ULID".to_owned())
}

#[derive(Clone, Debug)]
struct KnowledgeMcpServer<C: KnowledgeBackend> {
    client: C,
    concurrency: Arc<Semaphore>,
    shutdown: CancellationToken,
    tool_router: ToolRouter<Self>,
}

impl<C: KnowledgeBackend> KnowledgeMcpServer<C> {
    fn new(client: C, shutdown: CancellationToken) -> Self {
        Self {
            client,
            concurrency: Arc::new(Semaphore::new(MAXIMUM_CONCURRENT_OPERATIONS)),
            shutdown,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl<C: KnowledgeBackend> KnowledgeMcpServer<C> {
    #[tool(
        name = "knowledge_list",
        description = "List committed Agent Knowledge documents in canonical path order.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn list(
        &self,
        Parameters(parameters): Parameters<ReadParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client.with_cancellation(cancellation).list(&request)
            })
            .await?,
        )
    }

    #[tool(
        name = "knowledge_recent",
        description = "List recently committed Agent Knowledge documents, newest first.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn recent(
        &self,
        Parameters(parameters): Parameters<ReadParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client.with_cancellation(cancellation).recent(&request)
            })
            .await?,
        )
    }

    #[tool(
        name = "knowledge_search",
        description = "Search committed Agent Knowledge Markdown and permitted metadata.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn search(
        &self,
        Parameters(parameters): Parameters<SearchParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let request = parameters.into_request()?;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client.with_cancellation(cancellation).search(&request)
            })
            .await?,
        )
    }

    #[tool(
        name = "knowledge_get",
        description = "Get one committed Markdown document by permanent document ID.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn get(
        &self,
        Parameters(parameters): Parameters<DocumentParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let request = get_request(parameters.document_id)?;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client.with_cancellation(cancellation).get(&request)
            })
            .await?,
        )
    }

    #[tool(
        name = "knowledge_request_status",
        description = "Get the durable queue state of an accepted Agent Knowledge request.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    async fn request_status(
        &self,
        Parameters(parameters): Parameters<StatusParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let request = status_request(parameters.request_id)?;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client.with_cancellation(cancellation).status(&request)
            })
            .await?,
        )
    }

    #[tool(
        name = "knowledge_submit_package",
        description = "Validate and submit one local immutable Agent Knowledge request package over SSH.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn submit_package(
        &self,
        Parameters(parameters): Parameters<SubmitParameters>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        if parameters.package_root.is_empty() {
            return Err("package_root must not be empty".to_owned());
        }
        let package_root = parameters.package_root;
        let client = self.client.clone();
        structured(
            self.run_blocking(context, move |cancellation| {
                client
                    .with_cancellation(cancellation)
                    .submit(Path::new(&package_root))
            })
            .await?,
        )
    }
}

impl<C: KnowledgeBackend> KnowledgeMcpServer<C> {
    async fn run_blocking<T, E>(
        &self,
        context: RequestContext<RoleServer>,
        operation: impl FnOnce(CancellationFlag) -> Result<T, E> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
        E: fmt::Display + Send + 'static,
    {
        let request_cancellation = context.ct;
        let shutdown = self.shutdown.clone();
        let concurrency = self.concurrency.clone();
        let permit = tokio::select! {
            permit = concurrency.acquire_owned() => permit
                .map_err(|_| "MCP operation admission closed unexpectedly".to_owned())?,
            () = request_cancellation.cancelled() => {
                return Err("Agent Knowledge operation was cancelled".to_owned());
            }
            () = shutdown.cancelled() => {
                return Err("Agent Knowledge operation was cancelled".to_owned());
            }
        };

        if request_cancellation.is_cancelled() || shutdown.is_cancelled() {
            return Err("Agent Knowledge operation was cancelled".to_owned());
        }

        let cancellation = CancellationFlag::default();
        let worker_cancellation = cancellation.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(worker_cancellation)
        });
        tokio::select! {
            result = &mut task => result
                .map_err(|_| "local Agent Knowledge operation stopped unexpectedly".to_owned())?
                .map_err(|error| error.to_string()),
            () = request_cancellation.cancelled() => {
                cancel_operation(cancellation, task).await
            }
            () = shutdown.cancelled() => {
                cancel_operation(cancellation, task).await
            }
        }
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
                "Read committed knowledge with list, recent, search, and get. Submit only a complete local request package; the SSH destination and credentials are configured on this machine.",
            )
    }
}

async fn cancel_operation<T, E>(
    cancellation: CancellationFlag,
    task: tokio::task::JoinHandle<Result<T, E>>,
) -> Result<T, String>
where
    T: Send + 'static,
    E: fmt::Display + Send + 'static,
{
    cancellation.cancel();
    task.await
        .map_err(|_| "local Agent Knowledge operation stopped unexpectedly".to_owned())?
        .map_err(|error| error.to_string())?;
    Err("Agent Knowledge operation was cancelled".to_owned())
}

fn structured(value: impl Serialize) -> Result<CallToolResult, String> {
    serde_json::to_value(value)
        .map(CallToolResult::structured)
        .map_err(|_| "could not encode the Agent Knowledge response".to_owned())
}

pub(crate) fn run(client: SshClient) -> Result<(), McpServerError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(McpServerError::Runtime)?
        .block_on(async move {
            let shutdown = CancellationToken::new();
            let input = CancelOnEof::new(tokio::io::stdin(), shutdown.clone());
            let service = KnowledgeMcpServer::new(client, shutdown.clone())
                .serve((input, tokio::io::stdout()))
                .await
                .map_err(|error| McpServerError::Initialize(Box::new(error)))?;
            let result = service.waiting().await.map_err(McpServerError::Service);
            shutdown.cancel();
            result?;
            Ok(())
        })
}

struct CancelOnEof<R> {
    reader: R,
    shutdown: CancellationToken,
}

impl<R> CancelOnEof<R> {
    fn new(reader: R, shutdown: CancellationToken) -> Self {
        Self { reader, shutdown }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CancelOnEof<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled = buffer.filled().len();
        let remaining = buffer.remaining();
        let result = Pin::new(&mut self.reader).poll_read(context, buffer);
        if remaining > 0
            && matches!(&result, Poll::Ready(Ok(())))
            && buffer.filled().len() == filled
        {
            self.shutdown.cancel();
        }
        result
    }
}

impl<R> Drop for CancelOnEof<R> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug)]
pub enum McpServerError {
    Runtime(std::io::Error),
    Initialize(Box<rmcp::service::ServerInitializeError>),
    Service(tokio::task::JoinError),
}

impl fmt::Display for McpServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(formatter, "could not start the MCP runtime: {error}"),
            Self::Initialize(error) => write!(formatter, "could not initialize MCP: {error}"),
            Self::Service(error) => write!(formatter, "MCP service failed: {error}"),
        }
    }
}

impl std::error::Error for McpServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Initialize(error) => Some(error),
            Self::Service(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::{
        CancellationFlag, CancellationToken, KnowledgeBackend, KnowledgeMcpServer, ReadParameters,
        get_request, structured,
    };
    use agent_knowledge_protocol::{
        GetRequest, GetResponse, ListRequest, ListResponse, SearchRequest, StatusRequest,
        StatusResponse, SubmitResponse,
    };

    #[derive(Clone, Debug)]
    struct FakeBackend;

    impl KnowledgeBackend for FakeBackend {
        type Error = Infallible;

        fn with_cancellation(self, _cancellation: CancellationFlag) -> Self {
            self
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

        fn status(&self, _request: &StatusRequest) -> Result<StatusResponse, Self::Error> {
            unreachable!()
        }
    }

    #[test]
    fn advertises_the_expected_tool_set() {
        let server = KnowledgeMcpServer::new(FakeBackend, CancellationToken::new());
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
        let names = tools
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "knowledge_get",
                "knowledge_list",
                "knowledge_recent",
                "knowledge_request_status",
                "knowledge_search",
                "knowledge_submit_package",
            ]
        );
    }

    #[test]
    fn returns_structured_read_results() {
        let request = ReadParameters {
            project: None,
            tag: None,
            session: None,
            include_archived: false,
            maximum_results: None,
        }
        .into_request()
        .unwrap_or_else(|error| panic!("list parameters must be valid: {error}"));
        let response = FakeBackend
            .list(&request)
            .unwrap_or_else(|error| match error {});
        let result =
            structured(response).unwrap_or_else(|error| panic!("list result must encode: {error}"));
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
        let result = get_request("not-a-ulid".to_owned());
        assert_eq!(
            result.err().as_deref(),
            Some("document_id must be a canonical ULID")
        );
    }
}
