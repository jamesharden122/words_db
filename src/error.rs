use chrono::ParseError;
use csv::Error as CsvError;
use duckdb::arrow::error::ArrowError;
use duckdb::Error as DuckError;
use polars::error::PolarsError;
use serde_json::Error as SerdeError;
use std::io::Error as IoError;
use surrealdb::Error as SurrealError;
use tokio::task::JoinError;

#[derive(Debug)]
pub enum AppError {
    Surreal(SurrealError),
    Csv(CsvError),
    IoError(IoError),
    ArrowError(ArrowError),
    PolarsError(polars::error::PolarsError),
    ChronoError(ParseError),
    DuckError(DuckError),
    Serde(SerdeError),
    JoinError(JoinError),
    ParseError(ParseError),
    Utf8(std::str::Utf8Error),
    FromUtf8(std::string::FromUtf8Error),
    ParseInt(std::num::ParseIntError),
    ParseFloat(std::num::ParseFloatError),
    Poisoned(String),
}

// Implement From for surrealdb::Error
impl From<SurrealError> for AppError {
    fn from(err: SurrealError) -> Self {
        AppError::Surreal(err)
    }
}

// Implement From for csv::Error
impl From<CsvError> for AppError {
    fn from(err: CsvError) -> Self {
        AppError::Csv(err)
    }
}

impl From<PolarsError> for AppError {
    fn from(err: PolarsError) -> Self {
        AppError::PolarsError(err)
    }
}

// Implement From for std::io::Error
impl From<IoError> for AppError {
    fn from(err: IoError) -> Self {
        AppError::IoError(err)
    }
}

impl From<ArrowError> for AppError {
    fn from(err: ArrowError) -> Self {
        AppError::ArrowError(err)
    }
}

impl From<ParseError> for AppError {
    fn from(err: ParseError) -> Self {
        AppError::ChronoError(err)
    }
}

impl From<duckdb::Error> for AppError {
    fn from(err: DuckError) -> Self {
        Self::DuckError(err)
    }
}
impl From<tokio::task::JoinError> for AppError {
    fn from(err: JoinError) -> Self {
        Self::JoinError(err)
    }
}

impl From<SerdeError> for AppError {
    fn from(err: SerdeError) -> Self {
        Self::Serde(err)
    }
}

impl From<std::str::Utf8Error> for AppError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Utf8(err)
    }
}

impl From<std::string::FromUtf8Error> for AppError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::FromUtf8(err)
    }
}

impl From<std::num::ParseIntError> for AppError {
    fn from(err: std::num::ParseIntError) -> Self {
        Self::ParseInt(err)
    }
}

impl From<std::num::ParseFloatError> for AppError {
    fn from(err: std::num::ParseFloatError) -> Self {
        Self::ParseFloat(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for AppError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Self::Poisoned(err.to_string())
    }
}
