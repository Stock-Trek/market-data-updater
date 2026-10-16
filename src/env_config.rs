use std::env;

const DEFAULT_S3_BUCKET: &str = "stock-trek-market-data";
const DEFAULT_LAMBDA_FUNCTION_NAME: &str = "stock-trek-market-data";

pub struct EnvConfig {
    pub s3_bucket: String,
    pub lambda_function_name: String,
}

impl EnvConfig {
    pub fn from_env() -> Self {
        Self {
            s3_bucket: env::var("S3_BUCKET").unwrap_or_else(|_| DEFAULT_S3_BUCKET.to_string()),
            lambda_function_name: env::var("AWS_LAMBDA_FUNCTION_NAME")
                .or_else(|_| env::var("LAMBDA_FUNCTION_NAME"))
                .unwrap_or_else(|_| DEFAULT_LAMBDA_FUNCTION_NAME.to_string()),
        }
    }
}
