use crate::{constants::PARQUET_SUFFIX, s3, subfolders::get_subfolders_from_path};
use anyhow::{Ok, Result, anyhow};
use arrow::compute::concat;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReader};
use tokio::task::JoinSet;
use tracing::info;

struct SubfoldersFiles {
    pub subfolders: Vec<String>,
    pub files: Vec<String>,
}

pub async fn compact_all(
    s3_client: &S3Client,
    transfer_client: &TransferClient,
    bucket: &str,
) -> Result<()> {
    info!("compact all");
    compact_folder(&s3_client, &transfer_client, bucket, "", PARQUET_SUFFIX).await
}

async fn compact_folder(
    s3_client: &S3Client,
    transfer_client: &TransferClient,
    bucket: &str,
    prefix: &str,
    suffix: &str,
) -> Result<()> {
    info!("compact folder with prefix {}", prefix);
    let subfolders_files = find_subfolders_files(s3_client, bucket, prefix, suffix).await?;
    for subfolder in &subfolders_files.subfolders {
        let subfolder_full_path = format!("{}{}", prefix, subfolder);
        Box::pin(compact_folder(
            s3_client,
            transfer_client,
            bucket,
            &subfolder_full_path,
            suffix,
        ))
        .await?;
    }
    let expected_subfolders = get_subfolders_from_path(prefix)?;
    let expected_files = expected_subfolders
        .iter()
        .map(|f| format!("{}{}{}", prefix, f, suffix))
        .collect::<Vec<_>>();
    if expected_files.is_empty() {
        return Ok(());
    }
    let has_all = expected_files
        .iter()
        .filter(|expected| !subfolders_files.files.contains(expected))
        .collect::<Vec<_>>()
        .is_empty();
    if !has_all {
        return Ok(());
    }
    let compacted_file = format!("{}{}", prefix.trim_end_matches('/'), suffix);
    compact_files(
        s3_client,
        transfer_client,
        bucket,
        &expected_files,
        &compacted_file,
    )
    .await?;
    Ok(())
}

async fn find_subfolders_files(
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
                .map(|s| s.to_string()),
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

async fn compact_files(
    s3_client: &S3Client,
    transfer_client: &TransferClient,
    bucket: &str,
    files: &Vec<String>,
    compacted_file: &str,
) -> Result<()> {
    info!("compact files {:?} to {}", files, compacted_file);
    if files.is_empty() {
        println!("No data to compact");
        return Ok(());
    }
    let (batches, schema) = download_batches(transfer_client, bucket, files).await?;
    let combined_batch = combine_batches(&batches, &schema)?;
    let bytes = batch_to_bytes(&combined_batch, schema)?;
    s3::upload_bytes(transfer_client, &bucket, &compacted_file, bytes).await?;
    s3::delete_files(s3_client, &bucket, files).await?;
    Ok(())
}

async fn download_batches(
    transfer_client: &TransferClient,
    bucket: &str,
    files: &Vec<String>,
) -> Result<(Vec<RecordBatch>, SchemaRef)> {
    info!("download batches for files {:?}", files);
    if files.is_empty() {
        anyhow::bail!("No files provided");
    }
    let bucket_owned = bucket.to_string();
    let first_bytes = s3::download_bytes(transfer_client, &bucket_owned, &files[0]).await?;
    let mut first_reader = ParquetRecordBatchReader::try_new(first_bytes, 1024 * 1024)?;
    let schema = first_reader.next().unwrap()?.schema();
    let mut all_batches = first_reader.collect::<Result<Vec<RecordBatch>, _>>()?;
    let mut join_set: JoinSet<Result<Vec<RecordBatch>>> = JoinSet::new();
    for file_key in &files[1..] {
        let client = transfer_client.clone();
        let bucket_clone = bucket_owned.clone();
        let file_key = file_key.clone();
        join_set.spawn(async move {
            let batches = download_batches_for_key(&client, &bucket_clone, &file_key).await?;
            Ok(batches)
        });
    }
    while let Some(result) = join_set.join_next().await {
        let batches = result??;
        all_batches.extend(batches);
    }
    Ok((all_batches, schema))
}

async fn download_batches_for_key(
    transfer_client: &TransferClient,
    bucket: &str,
    key: &str,
) -> Result<Vec<RecordBatch>> {
    info!("download batches for key {}", key);
    let bytes = s3::download_bytes(&transfer_client, &bucket, &key).await?;
    let reader = ParquetRecordBatchReader::try_new(bytes, 1024 * 1024)?;
    let batches = reader.collect::<Result<Vec<RecordBatch>, _>>()?;
    Ok(batches)
}

fn combine_batches(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(anyhow!("No batches to combine"));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let num_columns = schema.fields().len();
    let mut combined_columns = vec![Vec::new(); num_columns];
    for batch in batches {
        for col_idx in 0..num_columns {
            combined_columns[col_idx].push(batch.column(col_idx).clone());
        }
    }
    let final_columns = combined_columns
        .into_iter()
        .map(|col_batches| {
            let arrays = col_batches
                .iter()
                .map(|arr| arr.as_ref())
                .collect::<Vec<_>>();
            concat(&arrays).expect("Failed to concatenate arrays")
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema.clone(), final_columns).map_err(Into::into)
}

fn batch_to_bytes(batch: &RecordBatch, schema: SchemaRef) -> Result<Bytes> {
    let mut vec = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut vec, schema, None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(Bytes::from(vec))
}
