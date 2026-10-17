use std::env;

use crate::error::{MduError, MduResult};

const S3_BUCKET_ENV_VAR: &str = "S3_BUCKET";
const LAMBDA_FUNCTION_NAME_ENV_VAR: &str = "LAMBDA_FUNCTION_NAME";

pub struct EnvConfig {
    pub s3_bucket: String,
    pub lambda_function_name: String,
}

impl EnvConfig {
    pub fn from_env() -> MduResult<Self> {
        Ok(Self {
            s3_bucket: Self::required_env_var(S3_BUCKET_ENV_VAR)?,
            lambda_function_name: Self::required_env_var(LAMBDA_FUNCTION_NAME_ENV_VAR)?,
        })
    }

    fn required_env_var(name: &'static str) -> MduResult<String> {
        env::var(name)
            .map_err(|_| MduError::MissingEnvVar { name })
            .and_then(|value| {
                if value.trim().is_empty() {
                    Err(MduError::EmptyEnvVar { name })
                } else {
                    Ok(value)
                }
            })
    }
}
