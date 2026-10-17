use std::{ops::Range, sync::Arc};

use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::error::MduError;

#[derive(Clone)]
pub struct ParquetReader {
    transfer_client: TransferClient,
    bucket: String,
    key: String,
    file_size: u64,
}

impl ParquetReader {
    pub fn new(transfer_client: &TransferClient, bucket: &str, key: &str, file_size: u64) -> Self {
        Self {
            transfer_client: transfer_client.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            file_size,
        }
    }

    async fn fetch_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let target = format!("s3://{}/{}", self.bucket, self.key);
        let mut handle = self
            .transfer_client
            .download()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(range_header)
            .initiate()
            .map_err(|error| {
                ParquetError::External(Box::new(MduError::s3(
                    "start ranged download",
                    target.clone(),
                    error,
                )))
            })?;
        let mut data = Vec::with_capacity((range.end - range.start) as usize);
        while let Some(chunk) = handle.body_mut().next().await {
            let chunk = chunk.map_err(|error| {
                ParquetError::External(Box::new(MduError::s3(
                    "read ranged download",
                    target.clone(),
                    error,
                )))
            })?;
            data.extend_from_slice(&chunk.data.into_bytes());
        }
        Ok(Bytes::from(data))
    }
}

impl AsyncFileReader for ParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move { self.fetch_range(range).await }.boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let reader = self.clone();
        async move {
            let fetches = ranges
                .into_iter()
                .map(|range| {
                    let reader = reader.clone();
                    async move { reader.fetch_range(range).await }
                })
                .collect::<Vec<_>>();
            futures::future::try_join_all(fetches).await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
