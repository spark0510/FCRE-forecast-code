faasr_met_data_bind <- function(config_set_name, configure_run_file){

  library(tidyverse)
  library(lubridate)
  a <- Sys.time()
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

  lake_directory <- here::here()

  message("Generate the `config_obs` object")
  config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name)

  message("Generate the `config` object")
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)


  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_met_station_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/7/02d36541de9088f2dd99d79dc3a7a853",
                       file = config_obs$met_raw_obs_fname[2],
                       lake_directory)


  cleaned_met_file <- met_data_bind(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[1]),
                                    qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[2]),
                                    cleaned_met_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id,paste0("observed-met_",config_obs$site_id,".csv")),
                                    input_file_tz = "EST",
                                    nldas = NULL,
                                    site_id = config_obs$site_id)


  FLAREr::put_targets(site_id = config_obs$site_id,
                      cleaned_met_file,
                      use_s3 = config$run_config$use_s3,
                      config = config)

  b <- Sys.time()

  readr::write_rds(a, "exec_start.RDS")
  readr::write_rds(b, "exec_end.RDS")
  FaaSr::faasr_put_file(remote_folder=paste0(.faasr$FaaSrLog, "/", .faasr$InvocationID, "/", .faasr$FunctionInvoke), 
                          remote_file="exec_start.RDS", 
                          local_file="exec_start.RDS")
  FaaSr::faasr_put_file(remote_folder=paste0(.faasr$FaaSrLog, "/", .faasr$InvocationID, "/", .faasr$FunctionInvoke), 
                          remote_file="exec_end.RDS", 
                          local_file="exec_end.RDS")

  d <- Sys.time()
  readr::write_rds(d, "trigger.RDS")
  FaaSr::faasr_put_file(remote_folder=paste0(.faasr$FaaSrLog, "/", .faasr$InvocationID, "/", .faasr$FunctionInvoke), 
                          remote_file="trigger.RDS", 
                          local_file="trigger.RDS")
}
