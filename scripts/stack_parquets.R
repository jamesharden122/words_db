library(arrow)
library(dplyr)

merge_parquet <- function(
    dir = "~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/monthly/"
) {
  # Get full paths to all parquet files in the directory
  files <- list.files(
    path = dir,
    pattern = "\\.parquet$",
    full.names = TRUE
  )
  
  if (length(files) == 0) {
    stop("No .parquet files found in directory: ", dir)
  }
  
  # Read all parquet files into a list of data frames / tibbles
  dt_list <- lapply(files, arrow::read_parquet)
  
  # Combine into one data frame
  dplyr::bind_rows(dt_list)
}

df = merge_parquet()
write_parquet(df,"~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/monthly_ff_factors.parquet")
df = merge_parquet("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/daily/")
write_parquet(df,"~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/daily_ff_factors.parquet")