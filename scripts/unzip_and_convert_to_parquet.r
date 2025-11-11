#' Convert ZIPs-with-CSV to ZIPs-with-Parquet
#'
#' For each .zip in `input_dir` (each expected to contain exactly one CSV),
#' this function:
#' 1) unzips to a temporary workspace,
#' 2) reads the CSV,
#' 3) writes a Parquet file using `save_parquet()` (provided by caller),
#' 4) deletes the CSV,
#' 5) re-zips the Parquet to `output_dir` using the original ZIP filename.
#'
#' The Parquet file name inside the new ZIP matches the original CSV's base name.
#'
#' @param input_dir Directory containing .zip files (each with a single CSV).
#' @param output_dir Directory to write the new ZIPs (can be the same as input).
#' @param csv_readr_args List of extra args passed to readr::read_csv()
#'   (e.g., list(guess_max = 100000)). Default keeps readr quiet & fast.
#' @param parquet_compression Parquet compression codec passed to save_parquet()
#'   (e.g., "zstd", "snappy", "gzip", "uncompressed").
#' @param parquet_compression_level Optional compression level for Parquet.
#' @param overwrite If TRUE, overwrite existing ZIPs in output_dir.
#' @return A tibble summarizing results (file, status, message).
#' @examples
#' # convert_zip_csvs_to_parquet_zip("data/zips_in", "data/zips_out")
source("save_parquet.r")

#' Convert one ZIP-with-CSV to a ZIP-with-Parquet
#'
#' Unzips a single input ZIP (containing exactly one CSV), converts the CSV to
#' Parquet using `save_parquet()`, and writes a replacement ZIP containing the
#' Parquet file.
#'
#' @param zip_in Path to the source .zip containing a single CSV.
#' @param dest_zip Path to the destination .zip to create/overwrite.
#' @param csv_readr_args List of extra args for readr::read_csv().
#' @param parquet_compression Compression codec for Parquet (e.g., "zstd").
#' @param parquet_compression_level Optional compression level.
#' @param overwrite If TRUE, overwrite `dest_zip` if it exists.
#' @return A list with `status` and `message`.
convert_zip_csv_to_parquet_zip <- function(zip_in,
                                           dest_zip,
                                           csv_readr_args = list(),
                                           parquet_compression = "zstd",
                                           parquet_compression_level = NULL,
                                           overwrite = TRUE) {
  if (!file.exists(zip_in)) {
    stop(sprintf("zip file not found: %s", zip_in))
  }
  if (!requireNamespace("readr", quietly = TRUE)) {
    stop("Package 'readr' is required. Install with install.packages('readr').")
  }
  if (!requireNamespace("zip", quietly = TRUE)) {
    stop("Package 'zip' is required. Install with install.packages('zip').")
  }
  if (!exists("save_parquet", mode = "function")) {
    stop("Could not find `save_parquet()` in the environment.")
  }

  # default readr options (quiet, no progress)
  csv_defaults <- list(show_col_types = FALSE, progress = FALSE)
  csv_args <- utils::modifyList(csv_defaults, csv_readr_args, keep.null = TRUE)

  work_dir <- tempfile(pattern = "zipcsv_")
  dir.create(work_dir)
  on.exit({ unlink(work_dir, recursive = TRUE, force = TRUE) }, add = TRUE)

  # 1) unzip
  utils::unzip(zip_in, exdir = work_dir)

  # 2) find the CSV (exactly one expected)
  csv_files <- list.files(work_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  if (length(csv_files) == 0) {
    stop("No CSV found inside ZIP.")
  }
  if (length(csv_files) > 1) {
    stop(sprintf("Expected 1 CSV, found %d: %s", length(csv_files), paste(basename(csv_files), collapse = ", ")))
  }
  csv_path <- csv_files[[1]]

  # base name for parquet (keep CSV name)
  parquet_name <- paste0(tools::file_path_sans_ext(basename(csv_path)), ".parquet")
  parquet_path <- file.path(work_dir, parquet_name)

  # 3) read CSV and write Parquet via save_parquet()
  df <- do.call(readr::read_csv, c(list(file = csv_path), csv_args))

  save_parquet(
    df = df,
    path = parquet_path,
    compression = parquet_compression,
    compression_level = parquet_compression_level,
    overwrite = TRUE
  )

  # 4) delete original CSV
  unlink(csv_path, force = TRUE)

  # 5) (re)zip the parquet; keep only the parquet (no directory nesting)
  # Create the temporary ZIP inside the writable work_dir, then move/copy to dest.
  # Use absolute paths for destination to avoid issues when working directory changes.
  dest_zip_abs <- tryCatch(normalizePath(dest_zip, winslash = "/", mustWork = FALSE), error = function(e) dest_zip)
  dest_dir_abs <- dirname(dest_zip_abs)
  if (!dir.exists(dest_dir_abs)) {
    ok <- dir.create(dest_dir_abs, recursive = TRUE, showWarnings = FALSE)
    if (!ok && !dir.exists(dest_dir_abs)) {
      stop(sprintf("Failed to create destination directory: %s", dest_dir_abs))
    }
  }

  tmp_zip <- tempfile(pattern = "parquet_zip_", tmpdir = work_dir, fileext = ".zip")

  owd <- getwd()
  setwd(work_dir)
  on.exit(setwd(owd), add = TRUE)
  zip::zipr(zipfile = tmp_zip, files = parquet_name, include_directories = FALSE)

  # If destination exists, handle overwrite policy only now (after successful zip).
  if (file.exists(dest_zip_abs)) {
    if (isTRUE(overwrite)) {
      unlink(dest_zip_abs, force = TRUE)
    } else {
      # Cleanup the temp zip and fail
      unlink(tmp_zip, force = TRUE)
      stop(sprintf("Destination ZIP already exists and overwrite = FALSE: %s", dest_zip_abs))
    }
  }

  # Try to move the temp zip into place atomically.
  if (!file.rename(tmp_zip, dest_zip_abs)) {
    # Fallback to file.copy + remove temp
    if (!file.copy(tmp_zip, dest_zip_abs, overwrite = FALSE)) {
      unlink(tmp_zip, force = TRUE)
      stop(sprintf("Failed to place output ZIP at %s", dest_zip_abs))
    }
    unlink(tmp_zip, force = TRUE)
  }

  list(status = "ok", message = sprintf("Created %s", dest_zip_abs))
}

convert_zip_csvs_to_parquet_zip <- function(input_dir,
                                            output_dir,
                                            csv_readr_args = list(),
                                            parquet_compression = "zstd",
                                            parquet_compression_level = NULL,
                                            overwrite = TRUE) {
  # ---- validation -----------------------------------------------------------
  if (!dir.exists(input_dir)) {
    stop(sprintf("`input_dir` does not exist: %s", input_dir))
  }
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  zip_paths <- list.files(input_dir, pattern = "\\.zip$", full.names = TRUE)
  if (length(zip_paths) == 0) {
    warning(sprintf("No .zip files found in %s", input_dir))
    return(tibble::tibble(zip_file = character(), status = character(), message = character()))
  }
  results <- vector("list", length(zip_paths))

  for (i in seq_along(zip_paths)) {
    zip_in <- zip_paths[[i]]
    zip_name <- basename(zip_in)
    dest_zip <- file.path(output_dir, zip_name)

    res <- try({
      convert_zip_csv_to_parquet_zip(
        zip_in = zip_in,
        dest_zip = dest_zip,
        csv_readr_args = csv_readr_args,
        parquet_compression = parquet_compression,
        parquet_compression_level = parquet_compression_level,
        overwrite = overwrite
      )
    }, silent = TRUE)

    if (inherits(res, "try-error")) {
      results[[i]] <- list(
        zip_file = zip_in,
        status = "error",
        message = as.character(attr(res, "condition")$message %||% res)
      )
    } else {
      results[[i]] <- list(
        zip_file = zip_in,
        status = res$status,
        message = res$message
      )
    }
  }

  tibble::as_tibble(do.call(rbind, lapply(results, as.data.frame, stringsAsFactors = FALSE)))
}

`%||%` <- function(x, y) if (is.null(x)) y else x
