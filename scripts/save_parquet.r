# Save a data.frame (or tibble) to a Parquet file using the arrow package.
#
# Usage:
#   install.packages("arrow")  # if not installed
#   df <- data.frame(a = 1:3, b = as.Date('2020-01-01') + 0:2)
#   save_parquet(df, "../data/tmp/example.parquet")
#
# Args:
#   df: data.frame/tibble/Arrow Table/RecordBatch.
#   path: output file path ending with .parquet
#   compression: "zstd" (default), "snappy", "gzip", or "uncompressed".
#   compression_level: optional integer level (depends on codec), e.g., 3–9.
#   overwrite: if FALSE, error when file exists.
#
# Returns: Invisibly returns the normalized output path.
save_parquet <- function(df,
                         path,
                         compression = "zstd",
                         compression_level = NULL,
                         overwrite = TRUE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required. Install with install.packages('arrow').")
  }

  if (!is.data.frame(df) && !inherits(df, "ArrowObject")) {
    stop("'df' must be a data.frame/tibble or an Arrow Table/RecordBatch.")
  }

  if (!nzchar(path)) stop("'path' must be a non-empty string ending with .parquet")
  if (!grepl("\\.parquet$", path, ignore.case = TRUE)) {
    warning("Output path does not end with .parquet; appending extension.")
    path <- paste0(path, ".parquet")
  }

  dir_path <- normalizePath(dirname(path), winslash = "/", mustWork = FALSE)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  if (file.exists(path) && !isTRUE(overwrite)) {
    stop(sprintf("File already exists: %s (set overwrite = TRUE to replace)", path))
  }

  args <- list(x = df, sink = path, compression = compression)
  if (!is.null(compression_level)) args$compression_level <- compression_level

  do.call(arrow::write_parquet, args)
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}

