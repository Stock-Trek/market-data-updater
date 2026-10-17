use std::error::Error as StdError;

use arrow_schema::ArrowError;
use parquet::errors::ParquetError;
use thiserror::Error;

pub type MduResult<T> = Result<T, MduError>;

#[derive(Debug, Error)]
pub enum MduError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("I/O error while {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("S3 {operation} failed for {target}: {source}")]
    S3 {
        operation: &'static str,
        target: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Failed to build S3 {operation} request for {target}: {source}")]
    S3Request {
        operation: &'static str,
        target: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Exchange request failed while trying to {operation}: {source}")]
    Exchange {
        operation: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("Background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("Concurrency semaphore was closed while waiting for a permit")]
    SemaphoreClosed,
    #[error("Unsupported exchange: {exchange}")]
    UnsupportedExchange { exchange: String },
    #[error("Unsupported trading mode: {trading_mode}")]
    UnsupportedTradingMode { trading_mode: String },
    #[error("Cannot find a symbol for trading pair {trading_pair}")]
    MissingSymbol { trading_pair: String },
    #[error("Unknown {exchange} submission outcome while fetching klines")]
    UnknownSubmissionOutcome { exchange: &'static str },
    #[error("Failed to initialize tracing: {message}")]
    Tracing { message: String },
    #[error("Failed to acquire lock {key}: another invocation is already running")]
    LockContention { key: String },
    #[error("Month {month} must be between 1 and 12")]
    InvalidMonth { month: u32 },
    #[error("Failed to parse year and month from path {path}")]
    InvalidYearMonth { path: String },
    #[error("Cannot get subfolders from path with too many delimiters {path}")]
    TooManyPathSegments { path: String },
    #[error("Date time out of range while trying to {operation}")]
    DateTimeOutOfRange { operation: &'static str },
}

impl MduError {
    pub fn io<E>(operation: &'static str, path: impl Into<String>, source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::Io {
            operation,
            path: path.into(),
            source: source.into(),
        }
    }

    pub fn s3<E>(operation: &'static str, target: impl Into<String>, source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::S3 {
            operation,
            target: target.into(),
            source: source.into(),
        }
    }

    pub fn s3_request<E>(operation: &'static str, target: impl Into<String>, source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::S3Request {
            operation,
            target: target.into(),
            source: source.into(),
        }
    }

    pub fn exchange<E>(operation: &'static str, source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::Exchange {
            operation,
            source: source.into(),
        }
    }
}
