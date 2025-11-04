library(readr)
library(dplyr)
library(tidyr)
library(purrr)

files <- list.files(pattern = "_country_returns\\.csv$")

wide <- files %>%
  map_dfr(~ read_csv(.x, show_col_types = FALSE) %>%
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
# write.csv(wide, "country_returns_wide.csv", row.names = FALSE)

