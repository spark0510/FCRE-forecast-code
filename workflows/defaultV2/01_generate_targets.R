
generate_targets <- function(config_set_name, configure_run_file, lake_directory){
  #renv::restore()

  library(tidyverse)
  library(lubridate)

  message("Beginning generate targets")
  message(config_set_name)

  # Set the lake directory to the repository directory

  #lake_directory <- here::here()

  # Source the R files in the repository

  #files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
  #sapply(files.sources, source)

  # Generate the `config_obs` object and create directories if necessary

  message("Generate the `config_obs` object")
  config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name)

  message("Generate the `config` object")
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

  #' Move targets to s3 bucket

  message("Successfully generated targets")

  FLAREr::put_targets(site_id = config_obs$site_id,
                      cleaned_insitu_file,
                      cleaned_met_file,
                      cleaned_inflow_file,
                      use_s3 = config$run_config$use_s3,
                      config = config)

  if(config$run_config$use_s3){
    message("Successfully moved targets to s3 bucket")
  }

  return(config)
}