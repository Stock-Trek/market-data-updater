use anyhow::Result;
use aws_sdk_s3::{
    Client as S3Client,
    error::{ProvideErrorMetadata, SdkError},
    primitives::ByteStream,
};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

const STALE_AFTER: Duration = Duration::from_secs(16 * 60);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub struct LockGuard {
    key: String,
    e_tag: String,
}

impl LockGuard {
    pub async fn acquire(s3_client: &S3Client, bucket: &str, key: &str) -> Result<Option<Self>> {
        let deadline = Instant::now() + ACQUIRE_TIMEOUT;
        loop {
            if let Some(guard) = Self::try_acquire(s3_client, bucket, key).await? {
                return Ok(Some(guard));
            }
            if Self::take_over_stale(s3_client, bucket, key).await? {
                continue;
            }
            if Instant::now() >= deadline {
                warn!("timed out waiting for lock {}", key);
                return Ok(None);
            }
            sleep(RETRY_INTERVAL).await;
        }
    }

    async fn try_acquire(s3_client: &S3Client, bucket: &str, key: &str) -> Result<Option<Self>> {
        match s3_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from_static(b"locked"))
            .send()
            .await
        {
            Ok(output) => {
                let e_tag = output.e_tag().unwrap_or_default().to_string();
                info!("acquired lock {} ({})", key, e_tag);
                Ok(Some(Self {
                    key: key.to_string(),
                    e_tag,
                }))
            }
            Err(error) if Self::is_precondition_failed(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn take_over_stale(s3_client: &S3Client, bucket: &str, key: &str) -> Result<bool> {
        let head = match s3_client.head_object().bucket(bucket).key(key).send().await {
            Ok(head) => head,
            Err(_) => return Ok(false),
        };
        let Some(last_modified) = head
            .last_modified()
            .and_then(|value| DateTime::from_timestamp(value.secs(), value.subsec_nanos()))
        else {
            return Ok(false);
        };
        if Utc::now().signed_duration_since(last_modified)
            < chrono::Duration::from_std(STALE_AFTER)?
        {
            return Ok(false);
        }
        let Some(e_tag) = head.e_tag() else {
            return Ok(false);
        };
        match s3_client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .if_match(e_tag)
            .send()
            .await
        {
            Ok(_) => {
                info!("removed stale lock {}", key);
                Ok(true)
            }
            Err(error) if Self::is_precondition_failed(&error) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn release(&self, s3_client: &S3Client, bucket: &str) -> Result<()> {
        match s3_client
            .delete_object()
            .bucket(bucket)
            .key(self.key.as_str())
            .if_match(self.e_tag.as_str())
            .send()
            .await
        {
            Ok(_) => {
                info!("released lock {}", self.key);
                Ok(())
            }
            Err(error) if Self::is_precondition_failed(&error) => {
                warn!("lock {} was taken over before release", self.key);
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn is_precondition_failed<E>(error: &SdkError<E>) -> bool
    where
        E: ProvideErrorMetadata,
    {
        if error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 412)
        {
            return true;
        }
        error
            .as_service_error()
            .and_then(|service_error| service_error.code())
            .is_some_and(|code| {
                code == "PreconditionFailed" || code == "ConditionalRequestConflict"
            })
    }
}
