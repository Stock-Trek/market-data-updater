use crate::{
    constants::{PARQUET_SUFFIX, UPDATE_QUANTIZATION, common_trading_pairs},
    parquet_writer::writer_properties,
    quantized_time::QuantizedTime,
    s3::S3,
    subfolders::SubFolders,
    updaters::{binance_updater::BinanceUpdater, exchange_updater::ExchangeUpdater},
};
use anyhow::Result;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use exchange_types::{klines::Kline, urls::TradingMode};
use parquet::arrow::ArrowWriter;
use std::{collections::HashMap, sync::Arc};
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::info;

pub struct KlinesUpdater {
    binance: HashMap<TradingMode, Arc<dyn ExchangeUpdater>>,
}

impl KlinesUpdater {
    pub fn try_new() -> Result<Self> {
        let mut binance: HashMap<TradingMode, Arc<dyn ExchangeUpdater>> = HashMap::new();
        for trading_mode in [TradingMode::Paper, TradingMode::Real] {
            binance.insert(
                trading_mode,
                Arc::new(BinanceUpdater::try_new(trading_mode)?),
            );
        }
        Ok(Self { binance })
    }

    pub async fn update(
        &self,
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
    ) -> Result<()> {
        info!("update");
        let now = Utc::now();
        let schema = Self::create_schema();
        let end_time = QuantizedTime::timestamp(now, UPDATE_QUANTIZATION);
        let mut join_set = JoinSet::new();
        for (cex_id_trading_mode, trading_pairs) in common_trading_pairs() {
            let s3_client = s3_client.clone();
            let transfer_client = transfer_client.clone();
            let bucket = bucket.to_string();
            let cex_id = cex_id_trading_mode.0;
            let trading_mode = cex_id_trading_mode.1;
            let schema = schema.clone();
            let updater = match cex_id {
                CexId::Binance => {
                    self.binance.get(&trading_mode).cloned().ok_or_else(|| {
                        anyhow::anyhow!("Unsupported trading mode: {trading_mode:?}")
                    })?
                }
                other => anyhow::bail!("Unsupported exchange: {other:?}"),
            };
            join_set.spawn(async move {
                Self::update_exchange(
                    &s3_client,
                    &transfer_client,
                    &bucket,
                    cex_id,
                    trading_mode,
                    trading_pairs,
                    &schema,
                    end_time,
                    updater.clone(),
                )
                .await
            });
        }
        while let Some(result) = join_set.join_next().await {
            result??;
        }
        Ok(())
    }

    async fn update_exchange(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &str,
        cex_id: CexId,
        trading_mode: TradingMode,
        trading_pairs: Vec<TradingPair>,
        schema: &SchemaRef,
        end_time: DateTime<Utc>,
        updater: Arc<dyn ExchangeUpdater>,
    ) -> Result<()> {
        info!("update exchange {:?} @ end time {}", cex_id, end_time);
        let start_time = QuantizedTime::previous_top(end_time, UPDATE_QUANTIZATION);
        let year = start_time.year();
        let month = start_time.month() as i32;
        let day = start_time.day() as i32;
        let hour = start_time.hour() as i32;
        let minute = start_time.minute() as i32;
        let candles_relative_file_path =
            SubFolders::file_path(&[year, month, day, hour, minute], PARQUET_SUFFIX);
        let compacted_relative_file_paths = [
            SubFolders::file_path(&[year, month, day, hour], PARQUET_SUFFIX),
            SubFolders::file_path(&[year, month, day], PARQUET_SUFFIX),
            SubFolders::file_path(&[year, month], PARQUET_SUFFIX),
            SubFolders::file_path(&[year], PARQUET_SUFFIX),
        ];
        let semaphore = Arc::new(Semaphore::new(10));
        let mut join_set = JoinSet::new();
        for trading_pair in trading_pairs {
            info!("update symbol {:?}", trading_pair);
            let bucket = bucket.to_string();
            let candles_relative_file_path = candles_relative_file_path.clone();
            let compacted_relative_file_paths = compacted_relative_file_paths.clone();
            let schema = schema.clone();
            let semaphore = semaphore.clone();
            let s3_client = s3_client.clone();
            let transfer_client = transfer_client.clone();
            let updater_clone = updater.clone();
            join_set.spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let candles_base_path = format!(
                    "{}/{}/{}/{}",
                    trading_mode, cex_id, trading_pair.base, trading_pair.quote
                );
                let candles_file_path =
                    format!("{}/{}", candles_base_path, candles_relative_file_path);
                if S3::has_file(&s3_client, &bucket, &candles_file_path).await? {
                    return Ok(());
                }
                for compacted_relative_file_path in &compacted_relative_file_paths {
                    let compacted_file_path =
                        format!("{}/{}", candles_base_path, compacted_relative_file_path);
                    if S3::has_file(&s3_client, &bucket, &compacted_file_path).await? {
                        return Ok(());
                    }
                }
                let candles = updater_clone
                    .klines(&trading_pair, start_time, end_time)
                    .await?;
                let candles_bytes = Self::candles_to_bytes(&candles, &schema)?;
                S3::upload_bytes(&transfer_client, &bucket, &candles_file_path, candles_bytes)
                    .await?;
                Ok::<_, anyhow::Error>(())
            });
        }
        while let Some(result) = join_set.join_next().await {
            result??;
        }
        Ok(())
    }

    fn create_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("open_time", DataType::Int64, false),
            Field::new("open", DataType::Float64, true),
            Field::new("high", DataType::Float64, true),
            Field::new("low", DataType::Float64, true),
            Field::new("close", DataType::Float64, true),
            Field::new("volume", DataType::Float64, true),
        ]))
    }

    fn candles_to_bytes(candles: &[Kline], schema: &SchemaRef) -> Result<Bytes> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(candles.iter().map(|k| k.open_time.0))),
            Arc::new(Float64Array::from_iter(
                candles.iter().map(|k| f64::try_from(k.open).ok()),
            )),
            Arc::new(Float64Array::from_iter(
                candles.iter().map(|k| f64::try_from(k.high).ok()),
            )),
            Arc::new(Float64Array::from_iter(
                candles.iter().map(|k| f64::try_from(k.low).ok()),
            )),
            Arc::new(Float64Array::from_iter(
                candles.iter().map(|k| f64::try_from(k.close).ok()),
            )),
            Arc::new(Float64Array::from_iter(
                candles.iter().map(|k| f64::try_from(k.volume).ok()),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        let bytes = Bytes::from(buffer);
        Ok(bytes)
    }
}
