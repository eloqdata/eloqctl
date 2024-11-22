mod cc_request {
    tonic::include_proto!("txservice.remote"); // The package name from your .proto file
}
use crate::cli::task::grpc::cc_request::{
    ClusterBackupResponse, CreateClusterBackupRequest, FetchClusterBackupRequest,
};
use cc_request::cc_rpc_service_client::CcRpcServiceClient;
use tonic::transport::Channel;

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
            backup_name,
            dest_host,
            dest_user,
            dest_path,
        });

        let response = self.client.create_cluster_backup(request).await?;
        Ok(response.into_inner())
    }

    pub async fn query_snapshot_status(
        &mut self,
        backup_name: String,
    ) -> Result<ClusterBackupResponse, tonic::Status> {
        let request = tonic::Request::new(FetchClusterBackupRequest { backup_name });

        let response = self.client.fetch_cluster_backup(request).await?;
        Ok(response.into_inner())
    }
}
