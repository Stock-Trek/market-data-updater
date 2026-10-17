use crate::{
    compacter::Compactor,
    constants::LOCK_KEY,
    env_config::EnvConfig,
    error::{MduError, MduResult},
    lock::LockGuard,
    s3::S3,
    updater::KlinesUpdater,
};
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::{Value, json};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt::Subscriber};

mod compacter;
mod constants;
mod env_config;
mod error;
mod lock;
mod parquet_reader;
mod parquet_writer;
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

fn setup_tracing() -> MduResult<()> {
    info!("set up tracing");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = Subscriber::builder()
        .with_ansi(false)
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).map_err(|error| MduError::Tracing {
        message: error.to_string(),
    })?;
    Ok(())
}

async fn function_handler(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    let env_config = EnvConfig::from_env();
    info!(
        "running lambda function {}",
        env_config.lambda_function_name
    );
    info!("get s3 and transfer clients");
    let (s3_client, transfer_client) = S3::clients().await;
    let bucket = env_config.s3_bucket.as_str();
    let Some(mut lock) = LockGuard::acquire(&s3_client, bucket, LOCK_KEY).await? else {
        info!("another invocation is already running; skipping");
        return Ok(json!({ "status": "skipped" }));
    };
    let result = process(&s3_client, &transfer_client, bucket).await;
    if let Err(release_error) = lock.release().await {
        error!("failed to release lock: {release_error}");
    }
    result?;
    Ok(json!({ "status": "ok" }))
}

async fn process(
    s3_client: &aws_sdk_s3::Client,
    transfer_client: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
) -> MduResult<()> {
    let updater = KlinesUpdater::try_new()?;
    updater.update(s3_client, transfer_client, bucket).await?;
    Compactor::all(s3_client, transfer_client, bucket).await?;
    Ok(())
}
