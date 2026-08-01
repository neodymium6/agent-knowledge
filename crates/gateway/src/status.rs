use std::time::Instant;

use agent_knowledge_protocol::{RequestStatus, StatusRequest, StatusResponse};
use agent_knowledge_queue::QueueRequestStatus;

use crate::{GatewayError, GatewaySettings};

pub(super) fn status(
    settings: &GatewaySettings,
    queue: &agent_knowledge_queue::QueueReader,
    request: StatusRequest,
    deadline: Instant,
) -> Result<super::read::PreparedResponse<StatusResponse>, GatewayError> {
    super::read::validate_version(request.protocol_version)?;
    let Some(observed) = queue
        .status_until(request.request_id, Some(deadline))
        .map_err(|error| GatewayError::Queue(Box::new(error)))?
    else {
        return Err(GatewayError::RequestNotFound {
            request_id: request.request_id,
        });
    };
    let status = match observed {
        QueueRequestStatus::Pending => RequestStatus::Pending,
        QueueRequestStatus::Processing => RequestStatus::Processing,
        QueueRequestStatus::Completed => RequestStatus::Completed,
        QueueRequestStatus::Failed {
            error_code,
            failed_at,
        } => RequestStatus::Failed {
            error_code,
            failed_at,
        },
    };
    super::read::prepare_response(
        settings,
        StatusResponse::new(request.request_id, status),
        deadline,
    )
}
