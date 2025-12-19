library(readr)
library(dplyr)
library(tidyr)
library(purrr)
library(arrow)
setwd("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/csv/country_indexes/monthly/returns/")
files <- list.files(pattern = "_country_returns\\.csv$")

wide <- files %>%
  map_dfr(~ read_csv(.x, show_col_types = FALSE) %>% 
            rename(portret = mportret, portretx = mportretx) %>%
            select(fic, date, portret, portretx) %>%
            mutate(date = as.Date(date))) %>%
  distinct(fic, date, portret, portretx) %>%              # guard against dup rows
  pivot_wider(
    id_cols = date,
    names_from = fic,
    values_from = c(portret, portretx),
    names_glue = "{fic}_{.value}",
    values_fill = NA
  ) %>%
  arrange(date)

# optional: save
colnames(wide) <- tolower(colnames(wide))
write.csv(wide, "../country_returns_wide.csv", row.names = FALSE)
setwd("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/country_indexes/monthly/")
write_parquet(wide,"country_returns_wide.parquet")

