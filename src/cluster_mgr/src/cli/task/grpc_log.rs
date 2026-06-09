// Include the auto-generated gRPC code for the LogService
pub mod log_proto {
    tonic::include_proto!("txlog");
}

// Re-export the types needed by other modules
use log_proto::log_service_client::LogServiceClient;
pub use log_proto::{AddPeerRequest, ChangePeersResponse, RemovePeerRequest};
use std::time::Duration;
use tonic::transport::Endpoint;
use tonic::{Request, Status};

async fn connect_log_service(
    address: &str,
) -> Result<LogServiceClient<tonic::transport::Channel>, Status> {
    let channel = Endpoint::new(address.to_string())
        .map_err(|e| Status::internal(format!("invalid log service endpoint: {}", e)))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .connect()
        .await
        .map_err(|e| Status::internal(format!("failed to connect to log service: {}", e)))?;
    Ok(LogServiceClient::new(channel))
}

/// Add a log peer via gRPC.
pub async fn add_log_peer(
    address: &str,
    request: AddPeerRequest,
) -> Result<ChangePeersResponse, Status> {
    let mut client = connect_log_service(address).await?;
    // Perform the AddPeer RPC
    let response = client.add_peer(Request::new(request)).await?;
    Ok(response.into_inner())
}

/// Remove a log peer via gRPC.
pub async fn remove_log_peer(
    address: &str,
    request: RemovePeerRequest,
) -> Result<ChangePeersResponse, Status> {
    let mut client = connect_log_service(address).await?;
    // Perform the RemovePeer RPC
    let response = client.remove_peer(Request::new(request)).await?;
    Ok(response.into_inner())
}
