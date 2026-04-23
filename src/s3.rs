use anyhow::Result;
use aws_sdk_s3::{
    Client as S3Client,
    types::{Delete, ObjectIdentifier},
};
use aws_sdk_s3_transfer_manager::{Client as TransferClient, io::InputStream};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

pub async fn clients() -> (S3Client, TransferClient) {
    let (s3_client, transfer_client) = tokio::join!(s3_client(), transfer_client());
    (s3_client, transfer_client)
}

async fn s3_client() -> S3Client {
    let config = aws_config::load_from_env().await;
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
) -> bool {
    match s3_client
        .head_object()
        .bucket(bucket.as_ref())
        .key(key.as_ref())
        .send()
        .await
    {
        Ok(_) => true,
        _ => false,
    }
}

#[allow(dead_code)]
pub async fn delete_file(
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

pub async fn delete_files(
    s3_client: &S3Client,
    bucket: &impl AsRef<str>,
    keys: &Vec<String>,
) -> Result<()> {
    for chunk in keys.chunks(1000) {
        let objects = chunk
            .iter()
            .map(|key| {
                ObjectIdentifier::builder()
                    .set_key(Some(key.clone()))
                    .build()
                    .map_err(|e| anyhow::anyhow!("Failed to build identifier for {}: {}", key, e))
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

pub async fn download_bytes(
    transfer_client: &TransferClient,
    bucket: &impl AsRef<str>,
    key: &impl AsRef<str>,
) -> Result<Bytes> {
    let mut handle = transfer_client
        .download()
        .bucket(bucket.as_ref())
        .key(key.as_ref())
        .initiate()?;
    let mut file_data = Vec::new();
    while let Some(chunk_result) = handle.body_mut().next().await {
        let chunk = chunk_result?.data.into_bytes();
        file_data.extend_from_slice(&chunk);
    }
    let bytes = Bytes::from(file_data);
    Ok(bytes)
}

pub async fn upload_bytes(
    transfer_client: &TransferClient,
    bucket: &impl AsRef<str>,
    key: &impl AsRef<str>,
    bytes: Bytes,
) -> Result<()> {
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
    file_path: &impl AsRef<str>,
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

#[allow(dead_code)]
pub async fn upload_file(
    transfer_client: &TransferClient,
    bucket: &impl AsRef<str>,
    key: &impl AsRef<str>,
    file_path: &impl AsRef<str>,
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
