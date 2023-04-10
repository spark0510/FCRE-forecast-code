source('R/met_data_combine.R')
library(tidyverse)
library(lubridate)
library(yaml)
library(EDIutils)

entity_names <- read_data_entity_names(packageId = 'edi.389.7')
met_edi <- read_data_entity(packageId = 'edi.389.7', entityId = entity_names$entityId[1])
met_edi <- readr::read_csv(file = met_edi)

L1_file <- read.csv('https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-metstation-data/FCRmet_L1.csv')

test_df <- met_data_bind(realtime_file = 'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-metstation-data/FCRmet_L1.csv',
                         qaqc_file = 'https://portal.edirepository.org/nis/dataviewer?packageid=edi.389.7&entityid=02d36541de9088f2dd99d79dc3a7a853',
                         cleaned_met_file = 'met_output',
                         input_file_tz = "America/New_York",
                         site_id = 'fcre',
                         nldas = NULL)
