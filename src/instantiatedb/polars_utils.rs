#[cfg(feature = "server")]
use polars::prelude::{DataFrame, PolarsResult};

#[cfg(feature = "server")]
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

#[cfg(feature = "server")]
pub fn df_to_cache_bytes(mut df: DataFrame) -> PolarsResult<Vec<u8>> {
    df.serialize_to_bytes()
}

#[cfg(feature = "server")]
pub fn df_from_cache_bytes(bytes: Vec<u8>) -> PolarsResult<DataFrame> {
    let mut cur = std::io::Cursor::new(bytes); // Cursor implements Read + Seek
    DataFrame::deserialize_from_reader(&mut cur)
}

#[cfg(feature = "server")]
pub fn save_cache(df: &DataFrame, path: &Path) -> PolarsResult<()> {
    let mut w = BufWriter::new(File::create(path)?);
    let mut df = df.clone();
    df.serialize_into_writer(&mut w)?;
    Ok(())
}

#[cfg(feature = "server")]
pub fn load_cache(path: &Path) -> PolarsResult<DataFrame> {
    let mut r = BufReader::new(File::open(path)?);
    DataFrame::deserialize_from_reader(&mut r)
}

