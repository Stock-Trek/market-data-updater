use anyhow::Result;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use digdigdig3::{
    AccountType, ExchangeId, MarketData, Symbol, connector_manager::ConnectorFactory, core::Kline,
};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::info;

use crate::{
    constants::{KLINE_LIMIT, PARQUET_SUFFIX, UPDATE_QUANTIZATION, common_symbols},
    quantized_time::quantized_timestamp,
    s3,
    subfolders::file_path,
};

pub async fn update(transfer_client: &TransferClient, bucket: &str, testnet: bool) -> Result<()> {
    info!("update, testnet={}", testnet);
    let now = Utc::now();
    let schema = create_schema();
    let top = quantized_timestamp(now, UPDATE_QUANTIZATION);
    let mut join_set = JoinSet::new();
    for (exchange_id, symbols) in common_symbols() {
        let transfer_client = transfer_client.clone();
        let bucket = bucket.to_string();
        let schema = schema.clone();
        join_set.spawn(async move {
            update_exchange(
                &transfer_client,
                &bucket,
                exchange_id,
                symbols,
                top,
                &schema,
                PARQUET_SUFFIX,
                testnet,
            )
            .await
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

async fn update_exchange(
    transfer_client: &TransferClient,
    bucket: &str,
    exchange_id: ExchangeId,
    symbols: Vec<Symbol>,
    top: DateTime<Utc>,
    schema: &SchemaRef,
    suffix: &str,
    testnet: bool,
) -> Result<()> {
    info!("update exchange {} @ {}", exchange_id, top);
    let connector = ConnectorFactory::create_public(exchange_id, testnet).await?;
    let year = top.year();
    let month = top.month() as i32;
    let day = top.day() as i32;
    let hour = top.hour() as i32;
    let minute = top.minute() as i32;
    let candles_relative_file_path = file_path(&[year, month, day, hour, minute], suffix);
    let exchange = exchange_id.as_str();
    let semaphore = Arc::new(Semaphore::new(10));
    let mut join_set = JoinSet::new();
    for symbol in symbols {
        info!("update symbol {}", symbol);
        let connector = connector.clone();
        let bucket = bucket.to_string();
        let candles_relative_file_path = candles_relative_file_path.clone();
        let schema = schema.clone();
        let semaphore = semaphore.clone();
        let transfer_client = transfer_client.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let candles_file_path = format!(
                "{}/{}/{}/{}",
                exchange, symbol.base, symbol.quote, candles_relative_file_path
            );
            let candles = connector
                .get_klines(symbol, "1m", KLINE_LIMIT, AccountType::Spot, None)
                .await?;
            let candles_bytes = candles_to_bytes(&candles, &schema)?;
            s3::upload_bytes(&transfer_client, &bucket, &candles_file_path, candles_bytes).await?;
            Ok::<_, anyhow::Error>(())
        });
    }
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    Ok(())
}

fn candles_to_bytes(candles: &[Kline], schema: &SchemaRef) -> Result<Bytes> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(candles.iter().map(|k| k.open_time))),
        Arc::new(Float64Array::from_iter(candles.iter().map(|k| k.open))),
        Arc::new(Float64Array::from_iter(candles.iter().map(|k| k.high))),
        Arc::new(Float64Array::from_iter(candles.iter().map(|k| k.low))),
        Arc::new(Float64Array::from_iter(candles.iter().map(|k| k.close))),
        Arc::new(Float64Array::from_iter(candles.iter().map(|k| k.volume))),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = Bytes::from(buffer);
    Ok(bytes)
}
