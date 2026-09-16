use anyhow::Result;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use exchange_gateway::clients::reqwest::ReqwestHttpClient;
use exchange_gateway::prelude::{Connector, SubmissionOutcome, TradingMode};
use exchange_types::binance::exchange::BinanceExchange;
use exchange_types::binance::klines::{BinanceKlinesInterval, BinanceKlinesRequest};
use exchange_types::binance::klines_limit::BinanceKlinesLimit;
use exchange_types::encode::ByteEncoder;
use exchange_types::klines::{Kline, KlinesResponse};
use exchange_types::new_types::Milliseconds;
use exchange_types::signer::Signer;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use std::time::Duration;
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::info;
use type_bridge::{BinanceConverter, Converter};

use crate::{
    constants::{KLINE_LIMIT, PARQUET_SUFFIX, TestReal, UPDATE_QUANTIZATION, common_symbols},
    quantized_time::{previous_top, quantized_timestamp},
    s3,
    subfolders::file_path,
};

type BinanceConnector = Connector<BinanceExchange, ReqwestHttpClient>;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;

pub async fn update(
    s3_client: &S3Client,
    transfer_client: &TransferClient,
    bucket: &str,
) -> Result<()> {
    info!("update");
    let now = Utc::now();
    let schema = create_schema();
    let end_time = quantized_timestamp(now, UPDATE_QUANTIZATION);
    let mut join_set = JoinSet::new();
    for (exchange_id_test_real, symbols) in common_symbols() {
        let s3_client = s3_client.clone();
        let transfer_client = transfer_client.clone();
        let bucket = bucket.to_string();
        let exchange_id = exchange_id_test_real.0;
        let test_real = exchange_id_test_real.1;
        let schema = schema.clone();
        join_set.spawn(async move {
            update_exchange(
                &s3_client,
                &transfer_client,
                &bucket,
                exchange_id,
                test_real,
                symbols,
                &schema,
                end_time,
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

fn create_binance_connector(testnet: bool) -> Result<BinanceConnector> {
    let trading_mode = if testnet {
        TradingMode::Paper
    } else {
        TradingMode::Real
    };
    let signer = Signer::new_unencrypted(ByteEncoder::HexLower);
    let connector = Connector::try_new_http_reqwest(
        trading_mode,
        BinanceExchange,
        signer,
        REQUEST_TIMEOUT,
        MAX_RETRIES,
    )?;
    Ok(connector)
}

async fn update_exchange(
    s3_client: &S3Client,
    transfer_client: &TransferClient,
    bucket: &str,
    cex_id: CexId,
    test_real: TestReal,
    trading_pairs: Vec<TradingPair>,
    schema: &SchemaRef,
    end_time: DateTime<Utc>,
) -> Result<()> {
    info!("update exchange {:?} @ end time {}", cex_id, end_time);
    match cex_id {
        CexId::Binance => {}
        other => anyhow::bail!("unsupported exchange: {other:?}"),
    }
    let testnet = test_real.into();
    let connector = Arc::new(create_binance_connector(testnet)?);
    let converter = BinanceConverter::default();
    let klines_limit = KLINE_LIMIT
        .map(|limit| BinanceKlinesLimit::try_new(u32::from(limit)))
        .transpose()?;
    let start_time = previous_top(end_time, UPDATE_QUANTIZATION);
    let candles_relative_file_path = file_path(
        &[
            start_time.year(),
            start_time.month() as i32,
            start_time.day() as i32,
            start_time.hour() as i32,
            start_time.minute() as i32,
        ],
        PARQUET_SUFFIX,
    );
    let exchange = cex_id.to_string();
    let semaphore = Arc::new(Semaphore::new(10));
    let mut join_set = JoinSet::new();
    for trading_pair in trading_pairs {
        info!("update symbol {:?}", trading_pair);
        let symbol = converter
            .ticker_mapping()
            .symbol(&trading_pair)
            .ok_or_else(|| anyhow::anyhow!("no Binance symbol for {trading_pair:?}"))?;
        let connector = connector.clone();
        let exchange = exchange.clone();
        let bucket = bucket.to_string();
        let candles_relative_file_path = candles_relative_file_path.clone();
        let schema = schema.clone();
        let semaphore = semaphore.clone();
        let s3_client = s3_client.clone();
        let transfer_client = transfer_client.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let candles_file_path = format!(
                "{}/{}/{}/{}/{}",
                test_real,
                exchange,
                trading_pair.base,
                trading_pair.quote,
                candles_relative_file_path
            );
            if s3::has_file(&s3_client, &bucket, &candles_file_path).await {
                return Ok(());
            }
            let mut request = BinanceKlinesRequest::new(symbol, BinanceKlinesInterval::ONE_MINUTE);
            request.endTime = Some(Milliseconds(end_time.timestamp_millis()));
            request.limit = klines_limit;
            let candles = get_klines(&connector, request).await?;
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

async fn get_klines(
    connector: &BinanceConnector,
    request: BinanceKlinesRequest,
) -> Result<Vec<Kline>> {
    let submission = connector.submit_http(request)?;
    match submission.wait().await? {
        SubmissionOutcome::Submitted(response) => Ok(response.klines()?),
        SubmissionOutcome::Unknown { .. } => {
            anyhow::bail!("unknown Binance klines submission outcome")
        }
    }
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
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = Bytes::from(buffer);
    Ok(bytes)
}
