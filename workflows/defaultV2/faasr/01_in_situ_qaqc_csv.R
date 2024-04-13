faasr_in_situ_qaqc_csv <- function(config_set_name, configure_run_file){

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
                      directory = config_obs$realtime_insitu_location,
                      git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/200/13/27ceda6bc7fdec2e7d79a6e4fe16ffdf",
                      file = config_obs$ctd_fname,
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/199/11/509f39850b6f95628d10889d66885b76",
                      file = config_obs$nutrients_fname,
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/11/81f396b3e910d3359907b7264e689052",
                      file = config_obs$secchi_fname,
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/551/7/38d72673295864956cccd6bbba99a1a3",
                      file = "Dissolved_CO2_CH4_Virginia_Reservoirs.csv",
                      lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/7/71e6b946b751aa1b966ab5653b01077f",
                      file = config_obs$insitu_obs_fname[2],
                      lake_directory)


  cleaned_insitu_file <- in_situ_qaqc_csv(insitu_obs_fname = file.path(config_obs$file_path$data_directory,config_obs$insitu_obs_fname),
                                          data_location = config_obs$file_path$data_directory,
                                          maintenance_file = file.path(config_obs$file_path$data_directory,config_obs$maintenance_file),
                                          ctd_fname = file.path(config_obs$file_path$data_directory, config_obs$ctd_fname),
                                          nutrients_fname =  file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                          secchi_fname = file.path(config_obs$file_path$data_directory, config_obs$secchi_fname),
                                          ch4_fname = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                          cleaned_insitu_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-insitu.csv")),
                                          lake_name_code = config_obs$site_id,
                                          config = config_obs)

  FLAREr::put_targets(site_id = config_obs$site_id,
                      cleaned_insitu_file
                      use_s3 = config$run_config$use_s3,
                      config = config)
}