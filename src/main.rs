use crate::{compacter::Compactor, constants::S3_BUCKET, s3::S3, updater::KlinesUpdater};
use exchange_types::urls::TradingMode;
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::{Value, json};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt::Subscriber};

mod compacter;
mod constants;
mod error;
mod quantized_time;
mod s3;
mod subfolders;
mod updater;
mod updaters;

#[tokio::main]
async fn main() -> Result<(), Error> {
    setup_tracing()?;
    run(service_fn(function_handler)).await
}

fn setup_tracing() -> Result<(), Error> {
    info!("set up tracing");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = Subscriber::builder()
        .with_ansi(false)
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
    Ok(())
}

async fn function_handler(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    info!("get s3 and transfer clients");
    let (s3_client, transfer_client) = S3::clients().await;
    let bucket = S3_BUCKET;
    let updater = KlinesUpdater::try_new(TradingMode::Paper)?;
    updater.update(&s3_client, &transfer_client, bucket).await?;
    Compactor::all(&s3_client, &transfer_client, bucket).await?;
    Ok(json!({ "status": "ok" }))
}
