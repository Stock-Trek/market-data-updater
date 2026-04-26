use crate::{compacter::compact_all, constants::S3_BUCKET, s3::clients, updater::update};
use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::{Value, json};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt::Subscriber};

mod compacter;
mod constants;
mod quantized_time;
mod s3;
mod subfolders;
mod updater;

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
    let (s3_client, transfer_client) = clients().await;
    let bucket = S3_BUCKET;
    let testnet = true;
    update(&s3_client, &transfer_client, bucket, testnet).await?;
    compact_all(&s3_client, &transfer_client, bucket).await?;
    Ok(json!({ "status": "ok" }))
}
