use lambda_runtime::{Error, LambdaEvent, run, service_fn};
use serde_json::Value;

async fn scheduled_task(_event: LambdaEvent<Value>) -> Result<(), Error> {
    tracing::info!("Hello");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt().json().with_target(false).init();
    run(service_fn(scheduled_task)).await
}
