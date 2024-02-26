library(tidyverse)
library(lubridate)

lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "bvre"
configure_run_file <- "configure_run.yml"
config_set_name <- "default"

source("R/in_situ_qaqc.R")
source("R/temp_oxy_chla_qaqc.R")
source("R/extract_secchi.R")
source("R/run_edi_data_bind.R")
#source("R/wq_realtime_edi_combine.R")


Sys.setenv("AWS_DEFAULT_REGION" = "renc",
           "AWS_S3_ENDPOINT" = "osn.xsede.org",
           "USE_HTTPS" = TRUE)

FLAREr::ignore_sigpipe()

noaa_ready <- TRUE

while(noaa_ready){

  message("Beginning generate targets")

  config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name = config_set_name)
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)


  dir.create(file.path(lake_directory, "targets", config_obs$site_id), showWarnings = FALSE)


  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_insitu_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_met_station_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_inflow_data_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/7/02d36541de9088f2dd99d79dc3a7a853",
                       file = config_obs$met_raw_obs_fname[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
                       file = config_obs$insitu_obs_fname[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/11/81f396b3e910d3359907b7264e689052",
                       file = config_obs$secchi_fname,
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/200/13/27ceda6bc7fdec2e7d79a6e4fe16ffdf",
                       file = config_obs$ctd_fname,
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76",
                       file = config_obs$nutrients_fname,
                       lake_directory)


  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/10/c065ff822e73c747f378efe47f5af12b",
                       file = config_obs$inflow_raw_file1[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95",
                       file = "silica_master_df.csv",
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/551/7/38d72673295864956cccd6bbba99a1a3",
                       file = "Dissolved_CO2_CH4_Virginia_Reservoirs.csv",
                       lake_directory)

  cleaned_insitu_file <- in_situ_qaqc(insitu_obs_fname = file.path(lake_directory,"data_raw", config_obs$insitu_obs_fname),
                                      data_location = file.path(lake_directory,"data_raw"),
                                      maintenance_file = file.path(lake_directory, "data_raw", config_obs$maintenance_file),
                                      ctd_fname = NA,
                                      nutrients_fname =  NA,
                                      secchi_fname = NA,
                                      cleaned_insitu_file = file.path(lake_directory,"targets", config_obs$site_id, paste0(config_obs$site_id,"-targets-insitu.csv")),
                                      site_id = config_obs$site_id,
                                      config = config_obs)

  FLAREr::put_targets(site_id = config_obs$site_id,
                      cleaned_insitu_file,
                      cleaned_met_file = NA,
                      cleaned_inflow_file = NA,
                      use_s3 = config$run_config$use_s3,
                      config = config)

  # Run FLARE
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
                             bucket = config$s3$warm_start$bucket,
                             endpoint = config$s3$warm_start$endpoint,
                             use_https = TRUE)

  RCurl::url.exists("https://hc-ping.com/a996a401-97b3-4884-a778-02243e056d2a", timeout = 5)

  noaa_ready <- FLAREr::check_noaa_present_arrow(lake_directory,
                                                 configure_run_file,
                                                 config_set_name = config_set_name)
}
