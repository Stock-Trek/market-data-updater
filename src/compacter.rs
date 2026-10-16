use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    constants::PARQUET_SUFFIX, parquet_reader::ParquetReader, parquet_writer::writer_properties,
    s3::S3, subfolders::SubFolders,
};
use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use futures::TryStreamExt;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use tokio::fs::File;
use tracing::{info, warn};

const READ_BATCH_SIZE: usize = 8192;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct Compactor;

struct SubfoldersFiles {
    pub subfolders: Vec<String>,
    pub files: Vec<String>,
}

impl Compactor {
    pub async fn all(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
    ) -> Result<()> {
        info!("compact all");
        Self::folder(s3_client, transfer_client, bucket, "", PARQUET_SUFFIX).await?;
        Ok(())
    }

    async fn folder(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
        prefix: &str,
        suffix: &str,
    ) -> Result<()> {
        info!("compact folder with prefix {}", prefix);
        let expected_subfolders = SubFolders::subfolders_from_path(prefix)?;
        if !expected_subfolders.is_empty() {
            let compacted_file = Self::compacted_file_path(prefix, suffix);
            if S3::has_file(s3_client, &bucket, &compacted_file).await? {
                info!("{} is already compacted", compacted_file);
                return Ok(());
            }
        }
        let subfolders_files = Self::subfolders_files(s3_client, bucket, prefix, suffix).await?;
        for subfolder in &subfolders_files.subfolders {
            let subfolder_full_path = format!("{}{}", prefix, subfolder);
            Box::pin(Self::folder(
                s3_client,
                transfer_client,
                bucket,
                &subfolder_full_path,
                suffix,
            ))
            .await?;
        }
        if expected_subfolders.is_empty() {
            info!("no expected subfolders for {}", prefix);
            return Ok(());
        }
        let expected_files = expected_subfolders
            .iter()
            .map(|f| format!("{}{}{}", prefix, f.strip_suffix('/').unwrap_or(f), suffix))
            .collect::<Vec<_>>();
        let missing = expected_files
            .iter()
            .filter(|expected| !subfolders_files.files.contains(expected))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            info!(
                "Compaction for {} is missing {}/{} files",
                prefix,
                missing.len(),
                expected_files.len()
            );
            return Ok(());
        }
        let compacted_file = Self::compacted_file_path(prefix, suffix);
        info!("compact files {:?} to {}", expected_files, compacted_file);
        Self::files(
            s3_client,
            transfer_client,
            bucket,
            &expected_files,
            &compacted_file,
        )
        .await?;
        Ok(())
    }

    fn compacted_file_path(prefix: &str, suffix: &str) -> String {
        format!("{}{}", prefix.trim_end_matches('/'), suffix)
    }

    async fn subfolders_files(
        s3_client: &S3Client,
        bucket: &str,
        prefix: &str,
        suffix: &str,
    ) -> Result<SubfoldersFiles> {
        info!("find subfolders and files for prefix {}", prefix);
        let mut list_request = s3_client.list_objects_v2().bucket(bucket).delimiter("/");
        if !prefix.is_empty() {
            list_request = list_request.prefix(prefix);
        }
        let mut paginator = list_request.into_paginator().send();
        let mut files = Vec::new();
        let mut subfolders = Vec::new();
        while let Some(page_result) = paginator.next().await {
            let page = page_result?;
            subfolders.extend(
                page.common_prefixes()
                    .iter()
                    .flat_map(|cp| cp.prefix())
                    .map(|s| s.strip_prefix(prefix).unwrap_or(s).to_string()),
            );
            for object in page.contents() {
                if let Some(key) = object.key()
                    && key.ends_with(suffix)
                {
                    files.push(key.to_string());
                }
            }
        }
        Ok(SubfoldersFiles { subfolders, files })
    }

    async fn files(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
        files: &[String],
        compacted_file: &str,
    ) -> Result<()> {
        info!("compact files {:?} to {}", files, compacted_file);
        if files.is_empty() {
            warn!("No data to compact");
            return Ok(());
        }
        if !Self::compact(s3_client, transfer_client, bucket, files, compacted_file).await? {
            warn!(
                "no non-empty data to compact for {:?}; removing empty files",
                files
            );
        }
        S3::delete_all(s3_client, &bucket, files).await?;
        Ok(())
    }

    async fn compact(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
        files: &[String],
        compacted_file: &str,
    ) -> Result<bool> {
        info!("compact {} files", files.len());
        let temp_path = Self::temp_file_path();
        let mut writer: Option<AsyncArrowWriter<File>> = None;
        let result = async {
            for file_key in files {
                let file_size = S3::object_size(s3_client, &bucket, file_key).await?;
                let reader = ParquetReader::new(transfer_client, bucket, file_key, file_size);
                let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await?
                    .with_batch_size(READ_BATCH_SIZE)
                    .build()?;
                while let Some(batch) = stream.try_next().await? {
                    if writer.is_none() {
                        let output = File::create(&temp_path).await?;
                        writer = Some(AsyncArrowWriter::try_new(
                            output,
                            batch.schema(),
                            Some(writer_properties()),
                        )?);
                    }
                    if let Some(writer) = writer.as_mut() {
                        writer.write(&batch).await?;
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            tokio::fs::remove_file(&temp_path).await.ok();
            return Err(error);
        }
        let Some(writer) = writer else {
            warn!("all parquet files for compaction are empty");
            return Ok(false);
        };
        writer.close().await?;
        S3::upload_file(transfer_client, &bucket, &compacted_file, &temp_path).await?;
        tokio::fs::remove_file(&temp_path).await.ok();
        Ok(true)
    }

    fn temp_file_path() -> std::path::PathBuf {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "market-data-updater-compaction-{}-{}.parquet",
            std::process::id(),
            counter
        ))
    }
}
