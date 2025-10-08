use chrono::ParseError;
use csv::Error as CsvError;
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
    PolarsError(polars::error::PolarsError),
    ChronoError(ParseError),
    DuckError(DuckError),
    Serde(SerdeError),
    JoinError(JoinError),
    ParseError(ParseError),
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
