# runs this while the most_recent != today
# with a while loop

library(tidyverse)
library(yaml)
setwd(here::here())


restart_file <- read_yaml(file.path(lake_directory,'/restart/fcre/test_runS3/configure_run.yml'))
most_recent_forecast <- restart_file$forecast_start_datetime

#updated_forecast_day <- paste((Sys.Date() - lubridate::days(6)), '00:00:00')
#updated_forecast_day <- "2023-02-26 00:00:00"
updated_forecast_day <- "2023-02-27"

counter <- 0

## Need to figure out the correct "forecast start day" to use. I was thinking it was current_day - 6 due to 5 day delay...ask Quinn

while (most_recent_forecast != updated_forecast_day | counter < 10) {

  print(counter)

  # runs the loop up to 10 times, don't want the GH action to time out
  source('workflows/defaultV2/combined_workflow.R')

  counter <- sum(counter, 1)

  # update most recent forecast day by reading in yaml file again
  restart_file <- read_yaml(file.path(lake_directory,'/restart/fcre/test_runS3/configure_run.yml'))
  most_recent_forecast <- restart_file$forecast_start_datetime

}
