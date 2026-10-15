use std::fmt;

use arrow_schema::ArrowError;

pub type MduResult<T> = Result<T, MduError>;

#[derive(Debug)]
pub enum MduError {
    EmptyParquet { key: String },
    NoBatches,
    Arrow(ArrowError),
    InvalidMonth { month: u32 },
    InvalidYearMonth { path: String },
    TooManyPathSegments { path: String },
}

impl fmt::Display for MduError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyParquet { key } => {
                write!(f, "Parquet file {} contains no record batches", key)
            }
            Self::NoBatches => write!(f, "No record batches to combine"),
            Self::Arrow(error) => write!(f, "Arrow error: {}", error),
            Self::InvalidMonth { month } => {
                write!(f, "Month {} must be between 1 and 12", month)
            }
            Self::InvalidYearMonth { path } => {
                write!(f, "Failed to parse year and month from path {}", path)
            }
            Self::TooManyPathSegments { path } => write!(
                f,
                "Cannot get subfolders from path with too many delimiters {}",
                path
            ),
        }
    }
}

impl std::error::Error for MduError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ArrowError> for MduError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}
