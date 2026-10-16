use crate::{
    constants::PARQUET_SUFFIX, parquet_writer::writer_properties, s3::S3, subfolders::SubFolders,
};
use anyhow::Result;
use arrow_array::RecordBatch;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReader};
use tokio::task::JoinSet;
use tracing::{info, warn};

const READ_BATCH_SIZE: usize = 8192;
const MAX_CONCURRENT_DOWNLOADS: usize = 8;

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
            .map(|f| {
                format!(
                    "{}{}{}",
                    prefix,
                    f.strip_suffix('/').or(Some(f)).unwrap(),
                    suffix
                )
            })
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
                if let Some(key) = object.key() {
                    if key.ends_with(suffix) {
                        files.push(key.to_string());
                    }
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
        let Some(bytes) = Self::compact(transfer_client, bucket, files).await? else {
            warn!(
                "no non-empty data to compact for {:?}; removing empty files",
                files
            );
            S3::delete_all(s3_client, &bucket, files).await?;
            return Ok(());
        };
        S3::upload_bytes(transfer_client, &bucket, &compacted_file, bytes).await?;
        S3::delete_all(s3_client, &bucket, files).await?;
        Ok(())
    }

    async fn compact(
        transfer_client: &TransferClient,
        bucket: &str,
        files: &[String],
    ) -> Result<Option<Bytes>> {
        info!("compact {} files", files.len());
        let mut writer: Option<ArrowWriter<Vec<u8>>> = None;
        let first_batches =
            Self::download_batches_for_key(transfer_client, bucket, &files[0]).await?;
        Self::write_batches(&mut writer, &first_batches)?;
        drop(first_batches);
        let bucket_owned = bucket.to_string();
        for chunk in files[1..].chunks(MAX_CONCURRENT_DOWNLOADS) {
            let mut join_set: JoinSet<Result<(usize, Vec<RecordBatch>)>> = JoinSet::new();
            for (offset, file_key) in chunk.iter().enumerate() {
                let client = transfer_client.clone();
                let bucket = bucket_owned.clone();
                let file_key = file_key.clone();
                join_set.spawn(async move {
                    let batches =
                        Self::download_batches_for_key(&client, &bucket, &file_key).await?;
                    Ok((offset, batches))
                });
            }
            let mut batches_by_offset = vec![None; chunk.len()];
            while let Some(result) = join_set.join_next().await {
                let (offset, batches) = result??;
                batches_by_offset[offset] = Some(batches);
            }
            for batches in batches_by_offset.into_iter().flatten() {
                Self::write_batches(&mut writer, &batches)?;
            }
        }
        let Some(writer) = writer else {
            warn!("all parquet files for compaction are empty");
            return Ok(None);
        };
        Ok(Some(Bytes::from(writer.into_inner()?)))
    }

    fn write_batches(
        writer: &mut Option<ArrowWriter<Vec<u8>>>,
        batches: &[RecordBatch],
    ) -> Result<()> {
        for batch in batches {
            if writer.is_none() {
                *writer = Some(ArrowWriter::try_new(
                    Vec::new(),
                    batch.schema(),
                    Some(writer_properties()),
                )?);
            }
            if let Some(writer) = writer.as_mut() {
                writer.write(batch)?;
            }
        }
        Ok(())
    }

    async fn download_batches_for_key(
        transfer_client: &TransferClient,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<RecordBatch>> {
        info!("download batches for key {}", key);
        let bytes = S3::download_bytes(transfer_client, &bucket, &key).await?;
        let reader = ParquetRecordBatchReader::try_new(bytes, READ_BATCH_SIZE)?;
        let batches = reader.collect::<Result<Vec<RecordBatch>, _>>()?;
        Ok(batches)
    }
}
