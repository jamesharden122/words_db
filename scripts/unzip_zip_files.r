#' Unzip a directory of ZIP files (optionally a subset)
#'
#' - Scans an input directory for .zip files (optionally recursively).
#' - Filters by explicit file names or a regex pattern.
#' - Extracts each ZIP into the output directory. By default it extracts
#'   directly into `output_dir` (no subdirectories). You can opt into
#'   subdirectories with `create_subdir_per_zip = TRUE`.
#'
#' This script can be sourced and the function called programmatically, or run
#' directly via Rscript with simple CLI flags (see examples below).
#'
#' @param input_dir Directory containing .zip files.
#' @param output_dir Directory where files are extracted.
#' @param files Optional character vector of ZIP basenames (or paths) to include.
#' @param match_regex Optional regex applied to ZIP basenames to include.
#' @param recursive If TRUE, search `input_dir` recursively.
#' @param limit Optional integer to process at most this many ZIPs.
#' @param overwrite If TRUE, overwrite existing extracted subdirectories.
#' @param create_subdir_per_zip If TRUE (default), extract each ZIP into its own
#'   subdirectory under `output_dir`; if FALSE, extract directly into
#'   `output_dir` (may cause filename collisions).
#' @param name_from_zip If TRUE, and the ZIP contains exactly one extracted file,
#'   rename/move that file to `ZIP_BASENAME` (minus ".zip") under `dest_dir`.
#'   If `add_extension` is set, the renamed file will use that extension.
#'
#' @return A data.frame (or tibble if available) summarizing results per ZIP.
#'
#' @examples
#' # Programmatic
#' # res <- unzip_zip_files(
#' #   input_dir = "data/zip/compustat/global_fundamentals",
#' #   output_dir = "data/unzipped/compustat/global_fundamentals",
#' #   match_regex = "_comp_g_fundq\\.zip$",
#' #   limit = 10,
#' #   overwrite = TRUE
#' # )
#'
#' # CLI
#' # Rscript scripts/unzip_zip_files.r \
#' #   --input=/path/to/zips \
#' #   --output=/path/to/out \
#' #   --pattern=_comp_g_fundq\\.zip$ \
#' #   --limit=25 --recursive --overwrite=true

unzip_zip_files <- function(input_dir,
                           output_dir,
                           files = NULL,
                           match_regex = NULL,
                           recursive = FALSE,
                           limit = NULL,
                           overwrite = TRUE,
                           create_subdir_per_zip = FALSE,
                           name_from_zip = FALSE,
                           add_extension = NULL) {
  # Validate inputs
  if (!dir.exists(input_dir)) {
    stop(sprintf("`input_dir` does not exist: %s", input_dir))
  }
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # Enumerate .zip files
  zip_paths <- list.files(input_dir, pattern = "\\.zip$", full.names = TRUE, recursive = isTRUE(recursive))

  # Optional filtering by explicit files list (compare by basename)
  if (!is.null(files) && length(files) > 0) {
    targets <- basename(files)
    zip_paths <- zip_paths[basename(zip_paths) %in% targets]
  }

  # Optional regex filtering by basename
  if (!is.null(match_regex) && nzchar(match_regex)) {
    keep <- grepl(match_regex, basename(zip_paths), perl = TRUE)
    zip_paths <- zip_paths[keep]
  }

  # Optional limit
  if (!is.null(limit)) {
    limit <- as.integer(limit)
    if (!is.na(limit) && limit > 0L && length(zip_paths) > limit) {
      zip_paths <- zip_paths[seq_len(limit)]
    }
  }

  # Nothing to do
  if (length(zip_paths) == 0) {
    msg <- sprintf("No .zip files matched in %s", input_dir)
    warning(msg)
    return(.as_tbl(data.frame(zip_file = character(), dest_dir = character(), n_files = integer(), status = character(), message = character())))
  }

  results <- vector("list", length(zip_paths))

  for (i in seq_along(zip_paths)) {
    zip_in <- zip_paths[[i]]
    zip_base <- basename(zip_in)

    # Determine extraction directory
    if (isTRUE(create_subdir_per_zip)) {
      dest_dir <- file.path(output_dir, tools::file_path_sans_ext(zip_base))
    } else {
      dest_dir <- output_dir
    }

    # Handle overwrite policy (only safe when creating subdirs)
    if (isTRUE(create_subdir_per_zip) && dir.exists(dest_dir)) {
      if (isTRUE(overwrite)) {
        unlink(dest_dir, recursive = TRUE, force = TRUE)
      } else {
        results[[i]] <- list(
          zip_file = zip_in,
          dest_dir = dest_dir,
          n_files = NA_integer_,
          status = "skipped",
          message = "Destination directory exists; overwrite = FALSE"
        )
        next
      }
    }

    # Ensure destination dir exists
    if (!dir.exists(dest_dir)) {
      dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
    }

    # Unzip
    res <- try(utils::unzip(zip_in, exdir = dest_dir, overwrite = isTRUE(overwrite)), silent = TRUE)
    if (inherits(res, "try-error")) {
      results[[i]] <- list(
        zip_file = zip_in,
        dest_dir = dest_dir,
        n_files = NA_integer_,
        status = "error",
        message = as.character(attr(res, "condition")$message %||% res)
      )
    } else {
      n_files <- length(res)

      # Resolve extracted paths to actual filesystem locations.
      # `utils::unzip()` may return:
      # - paths relative to `exdir` (e.g., "file.csv" or "dir/file.csv"), or
      # - paths that already include `exdir` (sometimes relative, sometimes absolute).
      extracted <- res
      extracted_full <- vapply(
        extracted,
        function(p) {
          if (file.exists(p)) return(p)
          file.path(dest_dir, p)
        },
        character(1)
      )
      finfo <- suppressWarnings(file.info(extracted_full))
      file_paths <- extracted_full[!is.na(finfo$isdir) & finfo$isdir == FALSE]

      # Optionally rename the (single) extracted file to match the ZIP basename
      n_zip_named <- 0L
      if (isTRUE(name_from_zip)) {
        if (length(file_paths) == 0) {
          results[[i]] <- list(
            zip_file = zip_in,
            dest_dir = dest_dir,
            n_files = as.integer(n_files),
            status = "error",
            message = "ZIP extracted no files to rename"
          )
          next
        }
        if (length(file_paths) != 1) {
          results[[i]] <- list(
            zip_file = zip_in,
            dest_dir = dest_dir,
            n_files = as.integer(n_files),
            status = "error",
            message = sprintf("name_from_zip=TRUE requires exactly 1 extracted file; found %d", length(file_paths))
          )
          next
        }

        src_fp <- file_paths[[1]]
        zip_stem <- tools::file_path_sans_ext(zip_base)
        ext <- ""
        if (!is.null(add_extension) && nzchar(add_extension)) {
          ext <- add_extension
        } else {
          src_ext <- tools::file_ext(src_fp)
          if (nzchar(src_ext)) ext <- paste0(".", src_ext)
        }
        dest_fp <- file.path(dest_dir, paste0(zip_stem, ext))

        if (!identical(normalizePath(src_fp, winslash = "/", mustWork = FALSE),
                       normalizePath(dest_fp, winslash = "/", mustWork = FALSE))) {
          if (file.exists(dest_fp)) {
            if (isTRUE(overwrite)) {
              unlink(dest_fp, force = TRUE)
            } else {
              results[[i]] <- list(
                zip_file = zip_in,
                dest_dir = dest_dir,
                n_files = as.integer(n_files),
                status = "skipped",
                message = sprintf("Destination exists; overwrite=FALSE: %s", dest_fp)
              )
              next
            }
          }

          ok <- try(suppressWarnings(file.rename(src_fp, dest_fp)), silent = TRUE)
          if (inherits(ok, "try-error") || !isTRUE(ok)) {
            ok2 <- try(file.copy(src_fp, dest_fp, overwrite = FALSE), silent = TRUE)
            if (inherits(ok2, "try-error") || !isTRUE(ok2)) {
              results[[i]] <- list(
                zip_file = zip_in,
                dest_dir = dest_dir,
                n_files = as.integer(n_files),
                status = "error",
                message = sprintf("Failed to rename/move extracted file to %s", dest_fp)
              )
              next
            }
            unlink(src_fp, force = TRUE)
          }
          n_zip_named <- 1L
        }
      }

      # Optionally append an extension (e.g., ".parquet") to each extracted file
      n_renamed <- 0L
      if (!isTRUE(name_from_zip) && !is.null(add_extension) && nzchar(add_extension)) {
        for (fp in file_paths) {
          if (endsWith(fp, add_extension)) next
          new_fp <- paste0(fp, add_extension)
          if (file.exists(new_fp)) {
            if (isTRUE(overwrite)) {
              unlink(new_fp, force = TRUE)
            } else {
              next
            }
          }
          ok <- try(suppressWarnings(file.rename(fp, new_fp)), silent = TRUE)
          if (!inherits(ok, "try-error") && isTRUE(ok)) n_renamed <- n_renamed + 1L
        }
      }
      results[[i]] <- list(
        zip_file = zip_in,
        dest_dir = dest_dir,
        n_files = as.integer(n_files),
        status = "ok",
        message = if (isTRUE(name_from_zip) && n_zip_named > 0L) {
          sprintf("Extracted %d files; renamed to ZIP basename", n_files)
        } else if (!is.null(add_extension) && nzchar(add_extension)) {
          sprintf("Extracted %d files; renamed %d with '%s'", n_files, n_renamed, add_extension)
        } else {
          sprintf("Extracted %d files", n_files)
        }
      )
    }
  }

  .as_tbl(do.call(rbind, lapply(results, as.data.frame, stringsAsFactors = FALSE)))
}

# Helper: tibble fallback
.as_tbl <- function(df) {
  if (requireNamespace("tibble", quietly = TRUE)) tibble::as_tibble(df) else df
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# Minimal CLI to run via Rscript
.parse_cli_args <- function(args) {
  out <- list()
  for (a in args) {
    if (!startsWith(a, "--")) next
    kv <- strsplit(sub("^--", "", a), "=", fixed = TRUE)[[1]]
    key <- kv[1]
    val <- if (length(kv) >= 2) kv[2] else ""
    out[[key]] <- val
  }
  out
}

.as_bool <- function(x, default = FALSE) {
  if (is.null(x)) return(default)
  if (!nzchar(x)) return(TRUE)
  tolower(x) %in% c("1", "true", "t", "yes", "y")
}

.as_int <- function(x, default = NULL) {
  if (is.null(x) || !nzchar(x)) return(default)
  xi <- suppressWarnings(as.integer(x))
  if (is.na(xi)) default else xi
}

if (sys.nframe() == 0) {
  args <- .parse_cli_args(commandArgs(trailingOnly = TRUE))
  if (!is.null(args$input) && !is.null(args$output)) {
    files <- NULL
    if (!is.null(args$files) && nzchar(args$files)) {
      files <- strsplit(args$files, ",", fixed = TRUE)[[1]]
      files <- trimws(files)
    }
    match_regex <- if (!is.null(args$pattern)) args$pattern else NULL
    recursive <- .as_bool(args$recursive, FALSE)
    limit <- .as_int(args$limit, NULL)
    overwrite <- .as_bool(args$overwrite, TRUE)
    # Default to flattening (no subdirs) unless user sets --flatten=false
    flatten <- .as_bool(args$flatten, TRUE)
    name_from_zip <- .as_bool(args$`name-from-zip`, FALSE)

    res <- unzip_zip_files(
      input_dir = args$input,
      output_dir = args$output,
      files = files,
      match_regex = match_regex,
      recursive = recursive,
      limit = limit,
      overwrite = overwrite,
      create_subdir_per_zip = !flatten,
      name_from_zip = name_from_zip,
      add_extension = if (!is.null(args$`add-ext`)) args$`add-ext` else NULL
    )

    print(res)
  } else {
    cat("Usage:\n")
    cat("  Rscript scripts/unzip_zip_files.r --input=/path/to/zips --output=/path/to/out [--pattern=REGEX] [--files=a.zip,b.zip] [--limit=N] [--recursive] [--overwrite=true] [--flatten=true|false] [--name-from-zip=true|false] [--add-ext=.parquet]\n")
  }
}
