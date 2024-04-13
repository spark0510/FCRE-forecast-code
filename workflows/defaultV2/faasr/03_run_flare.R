faasr_run_flare <- function(config_set_name, configure_run_file){
     
  library(tidyverse)
  library(lubridate)

  lake_directory <- here::here()
  setwd(lake_directory)
  forecast_site <- "fcre"

  conf_files <- c("configure_flare.yml", "configure_run.yml", "depth_model_sd.csv", 
                  "glm3.nml", "observation_processing.yml", "observations_config.csv",
                  "parameter_calibration_config.csv", "states_config.csv")

  for (conf_file in conf_files){
    FaaSr::faasr_get_file(server_name="My_Minio_Bucket",
                        remote_folder="configuration/defaultV2", 
                        remote_file=conf_file, 
                        local_folder="configuration/defaultV2", 
                        local_file=conf_file)
  }    

  configure_run_file <- "configure_run.yml"
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

  FLAREr::get_targets(lake_directory, config)

  output <- FLAREr::run_flare(lake_directory = lake_directory,
                                configure_run_file = configure_run_file,
                                config_set_name = config_set_name)

  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1)
  start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) - lubridate::days(1)
  restart_file <- paste0(config$location$site_id,"-", (lubridate::as_date(forecast_start_datetime)- days(1)), "-",config$run_config$sim_name ,".nc")

  FLAREr::update_run_config2(lake_directory = lake_directory,
                              configure_run_file = configure_run_file,
                              restart_file = restart_file,
                              start_datetime = start_datetime,
                              end_datetime = NA,
                              forecast_start_datetime = forecast_start_datetime,
                              forecast_horizon = config$run_config$forecast_horizon,
                              sim_name = config$run_config$sim_name,
                              site_id = config$location$site_id,
                              configure_flare = config$run_config$configure_flare,
                              configure_obs = config$run_config$configure_obs,
                              use_s3 = config$run_config$use_s3,
                              server_name = config$s3$warm_start$server_name,
                              folder = config$s3$warm_start$folder
                              #bucket = config$s3$warm_start$bucket,
                              #endpoint = config$s3$warm_start$endpoint,
                              #use_https = TRUE
                              )

  RCurl::url.exists("https://hc-ping.com/a996a401-97b3-4884-a778-02243e056d2a", timeout = 5)

  noaa_ready <- FLAREr::check_noaa_present_arrow(lake_directory,
                                                  configure_run_file,
                                                  config_set_name = config_set_name)

}