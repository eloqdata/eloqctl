use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::Client as S3Client;
use std::time::Duration;
use tracing::info;

pub struct S3ClientBuilder;

impl S3ClientBuilder {
    pub async fn build(
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
        endpoint: Option<&str>,
    ) -> Result<S3Client> {
        let credentials = Credentials::new(access_key_id, secret_access_key, None, None, "eloqctl");

        let mut config_builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(Region::new(region.to_string()))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(Duration::from_secs(30))
                    .operation_attempt_timeout(Duration::from_secs(30))
                    .operation_timeout(Duration::from_secs(120))
                    .build(),
            );

        if let Some(endpoint_url) = endpoint {
            config_builder = config_builder.endpoint_url(endpoint_url.to_string());
        }

        let config = config_builder.build();
        let client = S3Client::from_conf(config);
        Ok(client)
    }
}

pub async fn delete_s3_object(client: &S3Client, bucket: &str, key: &str) -> Result<()> {
    info!("Checking if S3 object exists: s3://{}/{}", bucket, key);

    // Check if object exists first
    let head_result = client.head_object().bucket(bucket).key(key).send().await;

    match head_result {
        Ok(_) => {
            // Object exists, proceed with deletion
            info!(
                "S3 object exists, proceeding with deletion: s3://{}/{}",
                bucket, key
            );
        }
        Err(SdkError::ServiceError(service_err)) => {
            match service_err.err() {
                HeadObjectError::NotFound(_) => {
                    return Err(anyhow::anyhow!(
                        "S3 object does not exist: s3://{}/{}",
                        bucket,
                        key
                    ));
                }
                other_err => {
                    // Try to get more details from the error
                    let error_details = format!("{:?}", other_err);
                    return Err(anyhow::anyhow!(
                        "Failed to check if S3 object exists: s3://{}/{}: {}",
                        bucket,
                        key,
                        error_details
                    ));
                }
            }
        }
        Err(e) => {
            // Catch-all for any other error variants (ConstructionFailure, ResponseError, TimeoutError, etc.)
            return Err(anyhow::anyhow!(
                "Failed to check if S3 object exists: s3://{}/{}: {:?}",
                bucket,
                key,
                e
            ));
        }
    }

    info!("Deleting S3 object: s3://{}/{}", bucket, key);

    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("Failed to delete s3://{}/{}", bucket, key))?;

    info!("Successfully deleted S3 object: s3://{}/{}", bucket, key);
    Ok(())
}

/// List S3 objects with given prefix
/// Returns vector of object keys matching the prefix
pub async fn list_s3_objects(client: &S3Client, bucket: &str, prefix: &str) -> Result<Vec<String>> {
    info!("Listing S3 objects with prefix: s3://{}/{}", bucket, prefix);

    let mut objects = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response: ListObjectsV2Output = request.send().await.context(format!(
            "Failed to list objects in s3://{}/{}",
            bucket, prefix
        ))?;

        // contents() returns &[Object], not Option
        for object in response.contents() {
            if let Some(key) = object.key() {
                objects.push(key.to_string());
            }
        }

        // Check if there are more objects to fetch
        // is_truncated() returns Option<bool>
        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    info!(
        "Found {} objects with prefix s3://{}/{}",
        objects.len(),
        bucket,
        prefix
    );
    Ok(objects)
}

/// Copy S3 object from source to destination
/// Uses S3 copy operation (server-side copy, no download/upload)
pub async fn copy_s3_object(
    client: &S3Client,
    bucket: &str,
    source_key: &str,
    dest_key: &str,
) -> Result<()> {
    info!(
        "Copying S3 object: s3://{}/{} -> s3://{}/{}",
        bucket, source_key, bucket, dest_key
    );

    let copy_source = format!("{}/{}", bucket, source_key);

    client
        .copy_object()
        .bucket(bucket)
        .copy_source(copy_source)
        .key(dest_key)
        .send()
        .await
        .context(format!(
            "Failed to copy s3://{}/{} to s3://{}/{}",
            bucket, source_key, bucket, dest_key
        ))?;

    info!(
        "Successfully copied S3 object: s3://{}/{} -> s3://{}/{}",
        bucket, source_key, bucket, dest_key
    );
    Ok(())
}
