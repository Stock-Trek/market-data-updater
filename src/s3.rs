use std::path::Path;

use anyhow::Result;
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
    ) -> Result<bool> {
        info!("checking for file {}", key.as_ref());
        match s3_client
            .head_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(e)) if e.err().is_not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn object_size(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
    ) -> Result<u64> {
        info!("get size for file {}", key.as_ref());
        let output = s3_client
            .head_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await?;
        Ok(output.content_length().unwrap_or_default().max(0) as u64)
    }

    pub async fn list_files(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        prefix: &impl AsRef<str>,
    ) -> Result<Vec<String>> {
        info!("listing files with prefix {}", prefix.as_ref());
        let mut files = Vec::new();
        let mut paginator = s3_client
            .list_objects_v2()
            .bucket(bucket.as_ref())
            .prefix(prefix.as_ref())
            .into_paginator()
            .send();
        while let Some(page_result) = paginator.next().await {
            let page = page_result?;
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
    ) -> Result<()> {
        s3_client
            .delete_object()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .send()
            .await?;
        Ok(())
    }

    pub async fn delete_all(
        s3_client: &S3Client,
        bucket: &impl AsRef<str>,
        keys: &[String],
    ) -> Result<()> {
        info!("delete files {:?}", keys);
        for chunk in keys.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .set_key(Some(key.clone()))
                        .build()
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to build identifier for {}: {}", key, e)
                        })
                })
                .collect::<Result<Vec<ObjectIdentifier>, _>>()?;
            let delete = Delete::builder().set_objects(Some(objects)).build()?;
            s3_client
                .delete_objects()
                .bucket(bucket.as_ref())
                .delete(delete)
                .send()
                .await?;
        }
        Ok(())
    }

    pub async fn upload_bytes(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        bytes: Bytes,
    ) -> Result<()> {
        info!("upload bytes for key {}", key.as_ref());
        let stream = InputStream::from(bytes);
        transfer_client
            .upload()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .body(stream)
            .initiate()?
            .join()
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn download_file(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        file_path: &impl AsRef<Path>,
    ) -> Result<()> {
        let mut handle = transfer_client
            .download()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .initiate()?;
        let mut file = tokio::fs::File::create(file_path.as_ref()).await?;
        while let Some(chunk_result) = handle.body_mut().next().await {
            let chunk = chunk_result?.data.into_bytes();
            file.write_all(&chunk).await?;
        }
        Ok(())
    }

    pub async fn upload_file(
        transfer_client: &TransferClient,
        bucket: &impl AsRef<str>,
        key: &impl AsRef<str>,
        file_path: &impl AsRef<Path>,
    ) -> Result<()> {
        let stream = InputStream::from_path(file_path.as_ref())?;
        transfer_client
            .upload()
            .bucket(bucket.as_ref())
            .key(key.as_ref())
            .body(stream)
            .initiate()?
            .join()
            .await?;
        Ok(())
    }
}
