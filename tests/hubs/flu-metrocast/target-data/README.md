# time-series.csv creation

We created the test [time-series.csv](time-series.csv) file from https://github.com/reichlab/flu-metrocast/ in early 2025-04-29 via these commands:

```r
readr::read_csv("https://github.com/reichlab/flu-metrocast/raw/e149b9212de7122a51257f8b08a6f780c6c567d9/target-data/time-series.csv") |>
  dplyr::filter(target_end_date >= "2025-02-01" & as_of < "2025-03-01") |>
  readr::write_csv("tests/hubs/flu-metrocast/target-data/time-series.csv")
```
