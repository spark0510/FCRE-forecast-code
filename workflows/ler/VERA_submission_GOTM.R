library(tidyverse)
library(arrow)
library(lubridate)


Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")


sites <- 'fcre'
flare_model_name <- 'GOTM'
vera_model_name <- 'flareGOTM'
force <- FALSE


# get the forecast from the FLARE bucket
forecasts <- arrow::s3_bucket(bucket = "forecasts/parquet",
                              endpoint_override = "s3.flare-forecast.org",
                              anonymous = TRUE)

this_year <- as.character(paste0(seq.Date(as_date('2024-01-01'), Sys.Date(), by = 'day'), ' 00:00:00'))

# check for missed submissions
flare_dates  <- arrow::open_dataset(forecasts) |>
  dplyr::filter(site_id %in% sites,
                reference_datetime %in% this_year,
                model_id == flare_model_name) |>
  dplyr::distinct(reference_datetime) |>
  dplyr::pull(as_vector = T)

flare_dates <- as_datetime(sort(flare_dates))

# Get all the submissions
submissions <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/vera4cast/forecasts/parquet/project_id=vera4cast/duration=P1D/variable=Temp_C_mean?endpoint_override=renc.osn.xsede.org") |>
  filter(model_id == vera_model_name) |>
  distinct(reference_datetime) |>
  pull()

# which depths have observations and will be evaluated in VERA
targets <- read_csv("https://renc.osn.xsede.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz") |>
  filter(variable == 'Temp_C_mean',
         site_id == sites)

eval_depths <- unique(targets$depth_m)

# are these dates in the challenge?
for (i in 1:length(flare_dates)) {

  forecast_file <- paste0(vera_model_name, '-', as_datetime(flare_dates[i]), '.csv.gz')

  exists <- flare_dates[i] %in% as_date(submissions)

  if (exists == T & force == F) {
    message(forecast_file, ' already submitted')
  }
  if (exists == F | (exists == T & force == T)) {
    message(forecast_file, ' missing')

    open_ds <- arrow::open_dataset(forecasts) %>%
      dplyr::filter(site_id %in% sites,
                    datetime > as_datetime(flare_dates[i]),
                    model_id == flare_model_name,
                    variable == 'temperature') %>%
      dplyr::collect() |>
      dplyr::filter(as.character(reference_datetime) == paste(as.character(flare_dates[i]), "00:00:00"))

    challenge_submission <- open_ds %>%
      # FLARE output at multiple depths
      # Need only the depths for which there are observations
      dplyr::filter(depth %in% eval_depths) %>%
      dplyr::mutate(model_id = vera_model_name,
                    reference_datetime = gsub(' 00:00:00', '', reference_datetime))%>%
      dplyr::rename(depth_m = depth) |>
      dplyr::select(c('datetime', 'reference_datetime', 'site_id', 'family', 'depth_m',
                      'parameter', 'variable', 'prediction', 'model_id')) |>
      dplyr::mutate(duration = 'P1D',
                    project_id = 'vera4cast')


    readr::write_csv(challenge_submission, forecast_file)
    # Submit forecast!

    # Now we can submit the forecast output to VERA
    vera4castHelpers::submit(forecast_file = forecast_file, ask = F)
    message('submitting missed forecast from: ', forecast_file)
  }
}
