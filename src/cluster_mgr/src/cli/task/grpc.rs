#[allow(clippy::enum_variant_names)]
pub(crate) mod cc_request {
    tonic::include_proto!("txservice.remote"); // The package name from your .proto file
}

use crate::cli::task::grpc::cc_request::{
    CheckCkptStatusRequest, CheckCkptStatusResponse, ClusterBackupResponse,
    ClusterRemoveNodeRequest, ClusterScaleResponse, CreateClusterBackupRequest,
    FetchClusterBackupRequest, NodeGroupAddPeersRequest, NotifyShutdownCkptRequest,
    NotifyShutdownCkptResponse,
};
use cc_request::{cc_rpc_service_client::CcRpcServiceClient, CkptStatus, ShutdownStatus};
use tonic::transport::Channel;
use tracing::{debug, error, info};

pub struct GrpcClient {
    client: CcRpcServiceClient<Channel>,
}

impl GrpcClient {
    pub async fn new(client_address: &str) -> Result<Self, tonic::transport::Error> {
        let client = CcRpcServiceClient::connect(client_address.to_string()).await?;
        Ok(Self { client })
    }

    pub async fn trigger_backup(
        &mut self,
        backup_name: String,
        dest_host: String,
        dest_user: String,
        dest_path: String,
    ) -> Result<ClusterBackupResponse, tonic::Status> {
        let request = tonic::Request::new(CreateClusterBackupRequest {
            backup_name: backup_name.clone(),
            dest_host,
            dest_user,
            dest_path,
        });

        let response = self.client.create_cluster_backup(request).await?;
        let response_inner = response.into_inner();

        if response_inner.result == "failed" {
            Err(tonic::Status::internal(format!(
                "Backup creation failed for '{}'",
                backup_name
            )))
        } else {
            Ok(response_inner)
        }
    }

    pub async fn query_snapshot_status(
        &mut self,
        backup_name: String,
    ) -> Result<ClusterBackupResponse, tonic::Status> {
        let request = tonic::Request::new(FetchClusterBackupRequest {
            backup_name: backup_name.clone(),
        });

        let response = self.client.fetch_cluster_backup(request).await?;
        let response_inner = response.into_inner();

        if response_inner.result == "failed" {
            Err(tonic::Status::internal(format!(
                "Failed to fetch backup status for '{}'",
                backup_name
            )))
        } else {
            Ok(response_inner)
        }
    }

    pub async fn trigger_ckpt(&mut self) -> Result<NotifyShutdownCkptResponse, tonic::Status> {
        let request = tonic::Request::new(NotifyShutdownCkptRequest {});

        let response = self.client.notify_shutdown_ckpt(request).await?;

        let response_inner = response.into_inner();

        match ShutdownStatus::from_i32(response_inner.status) {
            None => unreachable!(),
            Some(ShutdownStatus::ShutdownOngoing) => Err(tonic::Status::unknown(
                "Error: The shutdown process has already begun.",
            )),
            Some(ShutdownStatus::ShutdownFailed) => {
                Err(tonic::Status::unknown("Error: Leader transfered."))
            }
            Some(ShutdownStatus::ShutdownTriggered) => Ok(response_inner),
        }
    }

    pub async fn query_ckpt_status(
        &mut self,
        trigger_ckpt_ts: u64,
    ) -> Result<CheckCkptStatusResponse, tonic::Status> {
        let request = tonic::Request::new(CheckCkptStatusRequest { trigger_ckpt_ts });

        let response = self.client.check_ckpt_status(request).await?;

        let response_inner = response.into_inner();

        match CkptStatus::from_i32(response_inner.status) {
            None => unreachable!(),
            Some(CkptStatus::CkptFailed) => Err(tonic::Status::unknown(
                "An error occurred during shutdown checkpoint",
            )),
            _ => Ok(response_inner),
        }
    }

    /// Check if cluster scale has finished by event id
    pub async fn check_cluster_scale_status(
        &mut self,
        id: String,
    ) -> Result<cc_request::ClusterScaleStatusResponse, tonic::Status> {
        let request = tonic::Request::new(cc_request::ClusterScaleStatusRequest { id });
        let response = self.client.check_cluster_scale_status(request).await?;
        Ok(response.into_inner())
    }

    pub async fn add_peers(
        &mut self,
        request: NodeGroupAddPeersRequest,
    ) -> Result<ClusterScaleResponse, tonic::Status> {
        info!(
            "Preparing gRPC add_peers request with ng_id={}, id={}",
            request.ng_id, request.id
        );
        debug!("Full add_peers request details: {:?}", request);

        let req = tonic::Request::new(request);
        info!("Sending add_peers gRPC request...");

        match self.client.node_group_add_peers(req).await {
            Ok(response) => {
                let response_inner = response.into_inner();
                info!(
                    "Received add_peers response with result code: {}",
                    response_inner.result
                );
                debug!("Full add_peers response: {:?}", response_inner);
                Ok(response_inner)
            }
            Err(status) => {
                error!("add_peers request failed with gRPC status: {:?}", status);
                error!(
                    "Status code: {}, message: {}",
                    status.code(),
                    status.message()
                );
                Err(status)
            }
        }
    }

    pub async fn remove_node(
        &mut self,
        request: ClusterRemoveNodeRequest,
    ) -> Result<ClusterScaleResponse, tonic::Status> {
        info!(
            "Preparing gRPC remove_node request with id={}, node count={}",
            request.id, request.remove_node_count
        );
        debug!("Full remove_node request details: {:?}", request);

        let req = tonic::Request::new(request);
        info!("Sending remove_node gRPC request...");

        match self.client.cluster_remove_node(req).await {
            Ok(response) => {
                let response_inner = response.into_inner();
                info!(
                    "Received remove_node response with result code: {}",
                    response_inner.result
                );
                debug!("Full remove_node response: {:?}", response_inner);
                Ok(response_inner)
            }
            Err(status) => {
                error!("remove_node request failed with gRPC status: {:?}", status);
                error!(
                    "Status code: {}, message: {}",
                    status.code(),
                    status.message()
                );
                Err(status)
            }
        }
    }
}
