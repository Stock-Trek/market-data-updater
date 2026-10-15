use arrow_schema::ArrowError;
use thiserror::Error;

pub type MduResult<T> = Result<T, MduError>;

#[derive(Debug, Error)]
pub enum MduError {
    #[error("Parquet file {key} contains no record batches")]
    EmptyParquet { key: String },
    #[error("No record batches to combine")]
    NoBatches,
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Month {month} must be between 1 and 12")]
    InvalidMonth { month: u32 },
    #[error("Failed to parse year and month from path {path}")]
    InvalidYearMonth { path: String },
    #[error("Cannot get subfolders from path with too many delimiters {path}")]
    TooManyPathSegments { path: String },
}
