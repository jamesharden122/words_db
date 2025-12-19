setwd("/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/words_db/scripts/")
source("save_parquet.r")
source("unzip_zip_files.r")
source("unzip_and_convert_to_parquet.r")
setwd("/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/data/")

convert_zip_csvs_to_parquet_zip("zip/compustat/global_securities/monthly","zip/compustat/global_securities/monthly")
#convert_zip_csvs_to_parquet_zip("zip/factors/us/daily","zip/factors/us/daily")
#convert_zip_csvs_to_parquet_zip("zip/factors/us/monthly","zip/factors/us/monthly")
#convert_zip_csvs_to_parquet_zip("zip/crsp_ciz_sample/market_index/monthly","zip/crsp_ciz_sample/market_index/monthly")
#convert_zip_csvs_to_parquet_zip("zip/compustat/world_indicators/","zip/compustat/world_indicators/")
#convert_zip_csvs_to_parquet_zip("zip/compustat/global_fundamentals","zip/compustat/global_fundamentals")# "raw_files/parqueut/compustat/global_fundamentals")
#convert_zip_csvs_to_parquet_zip("zip/compustat/global_names","zip/compustat/global_names")# "raw_files/parqueut/compustat/global_names")
#unzip_zip_files(input_dir="zip/factors/us/daily", output_dir="raw_files/parquet/factors/us/daily", match_regex=".zip$", limit=10, create_subdir_per_zip = FALSE, overwrite=TRUE, add_extension = ".parquet")
#unzip_zip_files(input_dir="zip/factors/us/monthly", output_dir="raw_files/parquet/factors/us/monthly", match_regex=".zip$", limit=10, create_subdir_per_zip = FALSE, overwrite=TRUE, add_extension = ".parquet")
#unzip_zip_files(input_dir="zip/crsp_ciz_sample/market_index/monthly", output_dir="raw_files/parquet/crsp_ciz_sample/market_index/monthly", match_regex=".zip$", limit=10, create_subdir_per_zip = FALSE, overwrite=TRUE, add_extension = ".parquet")
#unzip_zip_files(input_dir="zip/compustat/world_indicators/", output_dir="raw_files/parquet/compustat/world_indicators/", match_regex=".zip$", limit=10, create_subdir_per_zip = FALSE, overwrite=TRUE, add_extension = ".parquet")
#unzip_zip_files(input_dir="zip/compustat/global_names", output_dir="raw_files/parquet/compustat/global_names", match_regex=".zip$", limit=10, create_subdir_per_zip = FALSE, overwrite=TRUE, add_extension = ".parquet")
unzip_zip_files(
  input_dir="zip/compustat/global_securities/monthly/",
  output_dir="raw_files/parquet/compustat/global_securities/monthly/",
  match_regex=".zip$",
  files = c(
    '(datadate__gte=2019-06-01&datadate__lte=2020-06-01)_comp_g_fundq.zip',
    '(datadate__gte=2020-06-01&datadate__lte=2021-06-01)_comp_g_fundq.zip',
    '(datadate__gte=2021-06-01&datadate__lte=2022-06-01)_comp_g_fundq.zip',
    "(datadate__gte=2022-06-01&datadate__lte=2023-06-01)_comp_g_fundq.zip",
    "(datadate__gte=2023-06-01&datadate__lte=2024-06-01)_comp_g_fundq.zip",
    "(datadate__gte=2024-06-01&datadate__lte=2025-06-01)_comp_g_fundq.zip",
    "(datadate__gte=2025-06-01&datadate__lte=2025-11-30)_comp_g_fundq.zip"
    ),
  limit=10,
  create_subdir_per_zip = FALSE,
  overwrite=TRUE,
  add_extension = ".parquet"
  )
# unzip_zip_files(
#   input_dir="zip/compustat/global_fundamentals/", 
#   output_dir="raw_files/parquet/compustat/global_fundamentals/", 
#   match_regex=".zip$", 
#   files = c(
#     '(datadate__gte=2019-06-01&datadate__lte=2020-06-01)_comp_g_fundq.zip',
#     '(datadate__gte=2020-06-01&datadate__lte=2021-06-01)_comp_g_fundq.zip',
#     '(datadate__gte=2021-06-01&datadate__lte=2022-06-01)_comp_g_fundq.zip',
#     "(datadate__gte=2022-06-01&datadate__lte=2023-06-01)_comp_g_fundq.zip",
#     "(datadate__gte=2023-06-01&datadate__lte=2024-06-01)_comp_g_fundq.zip",
#     "(datadate__gte=2024-06-01&datadate__lte=2025-06-01)_comp_g_fundq.zip",
#     "(datadate__gte=2025-06-01&datadate__lte=2025-11-30)_comp_g_fundq.zip"
#     ),
#   limit=10, 
#   create_subdir_per_zip = FALSE, 
#   overwrite=TRUE, 
#   add_extension = ".parquet"
#   )

# unzip_zip_files(
#   input_dir="zip/compustat/global_securities/", 
#   output_dir="raw_files/parquet/compustat/global_securities/", 
#   match_regex=".zip$", 
#   files = c(
#     "2019-01-01_2019-06-30_comp_global_daily.zip",
#     "2019-06-30_2020-01-01_comp_global_daily.zip",
#     "2020-01-01_2020-06-30_comp_global_daily.zip",
#     "2020-06-30_2021-01-01_comp_global_daily.zip",
#     "2021-01-01_2021-06-30_comp_global_daily.zip",
#     "2021-06-30_2022-01-01_comp_global_daily.zip",
#     "2022-01-01_2022-06-30_comp_global_daily.zip",
#     "2022-06-30_2023-01-01_comp_global_daily.zip",
#     "2023-01-01_2023-06-30_comp_global_daily.zip",
#     "2024-01-01_2024-03-30_comp_global_daily.zip",
#     "2024-03-30_2024-06-30_comp_global_daily.zip",
#     "2024-06-30_2024-09-15_comp_global_daily.zip",
#     "2024-09-15_2024-12-31_comp_global_daily.zip",
#     "2024-12-31_2025-03-15_comp_global_daily.zip",
#     "2025-03-15_2025-06-30_comp_global_daily.zip",
#     "2025-06-30_2025-08-30_comp_global_daily.zip"
#   ),
#   limit=17, 
#   create_subdir_per_zip = FALSE, 
#   overwrite=TRUE, 
#   add_extension = ".parquet"
# )