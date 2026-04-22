use crate::{compacter::compact_all, constants::S3_BUCKET, s3::clients, updater::update};

mod compacter;
mod constants;
mod quantized_time;
mod s3;
mod subfolders;
mod updater;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (s3_client, transfer_client) = clients().await;
    let bucket = S3_BUCKET;
    let testnet = true;
    update(&transfer_client, bucket, testnet).await?;
    compact_all(&s3_client, &transfer_client, bucket).await?;
    Ok(())
}
