faasr_inflow_data_combine <- function(config_set_name, configure_run_file){

  library(tidyverse)
  library(lubridate)

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
                      directory = config_obs$realtime_inflow_data_location,
                      git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/10/c065ff822e73c747f378efe47f5af12b",
                      file = config_obs$inflow_raw_file1[2],
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76",
                      file = config_obs$nutrients_fname,
                      lake_directory)                      

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95",
                      file = "silica_master_df.csv",
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/551/7/38d72673295864956cccd6bbba99a1a3",
                      file = "Dissolved_CO2_CH4_Virginia_Reservoirs.csv",
                      lake_directory)

  cleaned_inflow_file <- inflow_data_combine(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[1]),
                                          qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[2]),
                                          nutrients_file = file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                          silica_file = file.path(config_obs$file_path$data_directory,  config_obs$silica_fname),
                                          co2_ch4 = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                          cleaned_inflow_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-inflow.csv")),
                                          input_file_tz = 'EST',
                                          site_id = config_obs$site_id)

  FLAREr::put_targets(site_id = config_obs$site_id,
                      cleaned_inflow_file,
                      use_s3 = config$run_config$use_s3,
                      config = config)
}
