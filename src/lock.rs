use aws_sdk_s3::{
    Client as S3Client,
    error::{ProvideErrorMetadata, SdkError},
    primitives::ByteStream,
};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

use crate::error::{MduError, MduResult};

const STALE_AFTER: Duration = Duration::from_secs(16 * 60);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub struct LockGuard {
    s3_client: S3Client,
    bucket: String,
    key: String,
    e_tag: String,
    released: bool,
}

impl LockGuard {
    pub async fn acquire(s3_client: &S3Client, bucket: &str, key: &str) -> MduResult<Option<Self>> {
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

    async fn try_acquire(s3_client: &S3Client, bucket: &str, key: &str) -> MduResult<Option<Self>> {
        let target = Self::target(bucket, key);
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
                    s3_client: s3_client.clone(),
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    e_tag,
                    released: false,
                }))
            }
            Err(error) if Self::is_precondition_failed(&error) => Ok(None),
            Err(error) => Err(MduError::s3("acquire lock", target, error)),
        }
    }

    async fn take_over_stale(s3_client: &S3Client, bucket: &str, key: &str) -> MduResult<bool> {
        let target = Self::target(bucket, key);
        let head = match s3_client.head_object().bucket(bucket).key(key).send().await {
            Ok(head) => head,
            Err(SdkError::ServiceError(error)) if error.err().is_not_found() => return Ok(false),
            Err(error) => {
                return Err(MduError::s3("read lock metadata", target.clone(), error));
            }
        };
        let Some(last_modified) = head
            .last_modified()
            .and_then(|value| DateTime::from_timestamp(value.secs(), value.subsec_nanos()))
        else {
            return Ok(false);
        };
        let stale_after =
            chrono::Duration::from_std(STALE_AFTER).map_err(|_| MduError::DateTimeOutOfRange {
                operation: "convert lock staleness duration",
            })?;
        if Utc::now().signed_duration_since(last_modified) < stale_after {
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
            Err(error) => Err(MduError::s3("remove stale lock", target, error)),
        }
    }

    pub async fn release(&mut self) -> MduResult<()> {
        if self.released {
            return Ok(());
        }
        let result = Self::delete_lock(&self.s3_client, &self.bucket, &self.key, &self.e_tag).await;
        self.released = result.is_ok();
        result
    }

    async fn delete_lock(
        s3_client: &S3Client,
        bucket: &str,
        key: &str,
        e_tag: &str,
    ) -> MduResult<()> {
        let target = Self::target(bucket, key);
        match s3_client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .if_match(e_tag)
            .send()
            .await
        {
            Ok(_) => {
                info!("released lock {}", key);
                Ok(())
            }
            Err(error) if Self::is_precondition_failed(&error) => {
                warn!("lock {} was taken over before release", key);
                Ok(())
            }
            Err(error) => Err(MduError::s3("release lock", target, error)),
        }
    }

    fn target(bucket: &str, key: &str) -> String {
        format!("s3://{bucket}/{key}")
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

impl Drop for LockGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let s3_client = self.s3_client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let e_tag = self.e_tag.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                drop(handle.spawn(async move {
                    if let Err(error) =
                        LockGuard::delete_lock(&s3_client, &bucket, &key, &e_tag).await
                    {
                        warn!("failed to release lock {} on drop: {error}", key);
                    }
                }));
            }
            Err(_) => {
                warn!("no tokio runtime available to release lock {} on drop", key);
            }
        }
    }
}
