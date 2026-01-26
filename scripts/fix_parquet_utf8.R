#!/usr/bin/env Rscript

suppressPackageStartupMessages(library(arrow))

usage <- function() {
  cat(
    "Fix invalid UTF-8 strings in a Parquet file (for DuckDB).\n\n",
    "Usage:\n",
    "  Rscript scripts/fix_parquet_utf8.R <input.parquet> --inplace\n",
    "  Rscript scripts/fix_parquet_utf8.R <input.parquet> --out <output.parquet>\n\n",
    "Notes:\n",
    "  - Only character columns are checked/repaired.\n",
    "  - Repairs invalid strings by treating their bytes as Windows-1252 and converting to UTF-8.\n",
    sep = ""
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0 || any(args %in% c("-h", "--help"))) {
  usage()
  quit(status = if (length(args) == 0) 2 else 0)
}

in_path <- args[[1]]
out_path <- NA_character_
inplace <- FALSE

i <- 2
while (i <= length(args)) {
  a <- args[[i]]
  if (a == "--inplace") {
    inplace <- TRUE
    i <- i + 1
    next
  }
  if (a == "--out") {
    if (i == length(args)) stop("--out requires a value")
    out_path <- args[[i + 1]]
    i <- i + 2
    next
  }
  stop(paste0("Unknown argument: ", a))
}

if (is.na(out_path) && !inplace) {
  stop("Specify either --inplace or --out <output.parquet>")
}
if (!is.na(out_path) && inplace) {
  stop("Use only one of --inplace or --out")
}

if (!file.exists(in_path)) stop(paste0("Input not found: ", in_path))

utf8_valid_vec <- function(x) {
  if (!is.character(x)) return(rep(TRUE, length(x)))
  if (length(x) == 0) return(logical(0))
  converted <- suppressWarnings(iconv(x, from = "UTF-8", to = "UTF-8", sub = NA))
  is.na(x) | !is.na(converted)
}

fix_utf8_vec <- function(x) {
  if (!is.character(x)) return(x)

  ok <- utf8_valid_vec(x)
  if (all(ok)) return(enc2utf8(x))

  repaired <- x
  idx <- which(!ok)
  bytes <- x[idx]
  Encoding(bytes) <- "bytes"
  repaired[idx] <- iconv(bytes, from = "CP1252", to = "UTF-8", sub = "byte")
  enc2utf8(repaired)
}

cat("Reading:", in_path, "\n")
df <- read_parquet(in_path, as_data_frame = TRUE)
cat("Rows:", nrow(df), "Cols:", ncol(df), "\n")

char_cols <- names(df)[vapply(df, is.character, logical(1))]
if (length(char_cols) == 0) {
  cat("No character columns found; writing unchanged.\n")
} else {
  bad_cols <- 0L
  for (nm in char_cols) {
    x <- df[[nm]]
    bad <- which(!utf8_valid_vec(x))
    if (length(bad) > 0) {
      bad_cols <- bad_cols + 1L
      cat(sprintf("[bad utf8] column=%s bad_values=%d\n", nm, length(bad)))
      df[[nm]] <- fix_utf8_vec(x)
    }
  }
  if (bad_cols == 0L) cat("All character columns are valid UTF-8.\n")
}

if (inplace) {
  out_path <- in_path
  tmp_path <- paste0(in_path, ".tmp_utf8.parquet")
  cat("Writing temp:", tmp_path, "\n")
  write_parquet(df, tmp_path)
  ok <- file.rename(tmp_path, in_path)
  if (!ok) {
    unlink(tmp_path)
    stop("Failed to replace input file; temp file removed")
  }
  cat("Rewrote in-place:", in_path, "\n")
} else {
  cat("Writing:", out_path, "\n")
  write_parquet(df, out_path)
  cat("Wrote:", out_path, "\n")
}
