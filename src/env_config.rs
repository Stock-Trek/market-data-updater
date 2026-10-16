use std::env;

const S3_BUCKET_ENV_VAR: &str = "S3_BUCKET";
const LAMBDA_FUNCTION_NAME_ENV_VAR: &str = "LAMBDA_FUNCTION_NAME";

const DEFAULT_S3_BUCKET: &str = "stock-trek-market-data";
const DEFAULT_LAMBDA_FUNCTION_NAME: &str = "stock-trek-market-data";

pub struct EnvConfig {
    pub s3_bucket: String,
    pub lambda_function_name: String,
}

impl EnvConfig {
    pub fn from_env() -> Self {
        Self {
            s3_bucket: env::var(S3_BUCKET_ENV_VAR)
                .unwrap_or_else(|_| DEFAULT_S3_BUCKET.to_string()),
            lambda_function_name: env::var(LAMBDA_FUNCTION_NAME_ENV_VAR)
                .unwrap_or_else(|_| DEFAULT_LAMBDA_FUNCTION_NAME.to_string()),
        }
    }
}
