use std::path::Path;

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    error::SdkError,
    types::{Delete, ObjectIdentifier},
};
use aws_sdk_s3_transfer_manager::{Client as TransferClient, io::InputStream};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::error::{MduError, MduResult};

pub struct S3;

impl S3 {
    pub async fn clients() -> (S3Client, TransferClient) {
        let (s3_client, transfer_client) = tokio::join!(Self::s3_client(), Self::transfer_client());
        (s3_client, transfer_client)
    }

    async fn s3_client() -> S3Client {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        S3Client::new(&config)
    }

    async fn transfer_client() -> TransferClient {
        let config = aws_sdk_s3_transfer_manager::from_env().load().await;
        TransferClient::new(config)
    }

    pub async fn has_file(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
    ) -> MduResult<bool> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        info!("checking for file {}", key.as_ref());
        match s3_client
            .head_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(error)) if error.err().is_not_found() => Ok(false),
            Err(error) => Err(MduError::s3("check object existence", target, error)),
        }
    }

    pub async fn object_size(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
    ) -> MduResult<u64> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        info!("get size for file {}", key.as_ref());
        let output = s3_client
            .head_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await
            .map_err(|error| MduError::s3("read object metadata", target, error))?;
        Ok(output.content_length().unwrap_or_default().max(0) as u64)
    }

    pub async fn list_files(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        prefix: &impl AsRef<str>,
    ) -> MduResult<Vec<String>> {
        let target = Self::object_target(bucket.as_ref(), prefix.as_ref());
        info!("listing files with prefix {}", prefix.as_ref());
        let mut files = Vec::new();
        let mut paginator = s3_client
            .list_objects_v2()
            .bucket(bucket.as_ref())
            .prefix(prefix.as_ref())
            .into_paginator()
            .send();
        while let Some(page_result) = paginator.next().await {
            let page =
                page_result.map_err(|error| MduError::s3("list objects", target.clone(), error))?;
            for object in page.contents() {
                if let Some(key) = object.key() {
                    files.push(key.to_string());
                }
            }
        }
        Ok(files)
    }

    #[allow(dead_code)]
    pub async fn delete(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
    ) -> MduResult<()> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        s3_client
            .delete_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await
            .map_err(|error| MduError::s3("delete object", target, error))?;
        Ok(())
    }

    pub async fn delete_all(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        keys: &[String],
    ) -> MduResult<()> {
        info!("delete files {:?}", keys);
        for chunk in keys.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .set_key(Some(key.clone()))
                        .build()
                        .map_err(|error| {
                            MduError::s3_request(
                                "build delete identifier",
                                Self::object_target(bucket.as_ref(), key),
                                error,
                            )
                        })
                })
                .collect::<MduResult<Vec<ObjectIdentifier>>>()?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .build()
                .map_err(|error| {
                    MduError::s3_request(
                        "build delete request",
                        format!("s3://{}/{} objects", bucket.as_ref(), chunk.len()),
                        error,
                    )
                })?;
            s3_client
                .delete_objects()
                .bucket(bucket.as_ref())
                .delete(delete)
                .send()
                .await
                .map_err(|error| {
                    MduError::s3(
                        "delete objects",
                        format!("s3://{}/{} objects", bucket.as_ref(), chunk.len()),
                        error,
                    )
                })?;
        }
        Ok(())
    }

    pub async fn upload_bytes(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        bytes: Bytes,
    ) -> MduResult<()> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        info!("upload bytes for key {}", key.as_ref());
        let stream = InputStream::from(bytes);
        transfer_client
            .upload()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .body(stream)
            .initiate()
            .map_err(|error| MduError::s3("start upload", target.clone(), error))?
            .join()
            .await
            .map_err(|error| MduError::s3("upload object", target, error))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn download_file(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        file_path: &impl AsRef<Path>,
    ) -> MduResult<()> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        let file_path = file_path.as_ref();
        let path = file_path.display().to_string();
        let mut handle = transfer_client
            .download()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .initiate()
            .map_err(|error| MduError::s3("start download", target.clone(), error))?;
        let mut file = tokio::fs::File::create(file_path)
            .await
            .map_err(|error| MduError::io("create downloaded file", path.clone(), error))?;
        while let Some(chunk_result) = handle.body_mut().next().await {
            let chunk = chunk_result
                .map_err(|error| MduError::s3("read downloaded object", target.clone(), error))?
                .data
                .into_bytes();
            file.write_all(&chunk)
                .await
                .map_err(|error| MduError::io("write downloaded file", path.clone(), error))?;
        }
        Ok(())
    }

    pub async fn upload_file(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        file_path: &impl AsRef<Path>,
    ) -> MduResult<()> {
        let target = Self::object_target(bucket.as_ref(), key.as_ref());
        let stream = InputStream::from_path(file_path.as_ref()).map_err(|error| {
            MduError::io(
                "open file for upload",
                file_path.as_ref().display().to_string(),
                error,
            )
        })?;
        transfer_client
            .upload()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .body(stream)
            .initiate()
            .map_err(|error| MduError::s3("start upload", target.clone(), error))?
            .join()
            .await
            .map_err(|error| MduError::s3("upload object", target, error))?;
        Ok(())
    }

    fn object_target(bucket: &str, key: &str) -> String {
        format!("s3://{bucket}/{key}")
    }
}
