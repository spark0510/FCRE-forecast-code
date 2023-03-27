library(tidyverse)
library(arrow)

met_pull <- function(interest_var){
#select_dates <- seq(as.Date("2022-05-01"), as.Date("2022-05-10"), by="days")

select_dates <- as.Date("2022-05-01")
for (day in select_dates){
  date <- format(as.Date(day,origin="1970-01-01"))
  #print(date)
  s3_path <- paste0("drivers/noaa/gefs-v12-reprocess/stage1/0/",date,'/fcre')
  s3 <- arrow::s3_bucket(s3_path, endpoint_override = "s3.flare-forecast.org", anonymous = TRUE)
  df <- arrow::open_dataset(s3) %>% collect()

  noon_df <- df %>%
    mutate(time = format(as.POSIXct(datetime), format = "%H:%M")) %>%
    filter(time == '12:00')#, variable == interest_var)

  if (date == select_dates[1]){
    df_build <- noon_df
  }else{
    df_build <- rbind(df_build,noon_df)
  }

}

return(df_build)
}


df_pres <- met_pull('TMP')




### Pull data from EDI

# Package ID: edi.389.7 Cataloging System:https://pasta.edirepository.org.
# Data set title: Time series of high-frequency meteorological data at Falling Creek Reservoir, Virginia, USA 2015-2022.
# Data set creator:  Cayelan Carey - Virginia Tech
# Data set creator:  Adrienne Breef-Pilz - Virginia Tech
# Contact:  Cayelan Carey -  Virginia Tech  - Cayelan@vt.edu
# Stylesheet v2.11 for metadata conversion into program: John H. Porter, Univ. Virginia, jporter@virginia.edu

inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/389/7/02d36541de9088f2dd99d79dc3a7a853"
infile1 <- tempfile()
try(download.file(inUrl1,infile1,method="curl"))
if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")


dt1 <-read.csv(infile1,header=F
               ,skip=1
               ,sep=","
               , col.names=c(
                 "Reservoir",
                 "Site",
                 "DateTime",
                 "Record",
                 "CR3000Battery_V",
                 "CR3000Panel_Temp_C",
                 "PAR_umolm2s_Average",
                 "PAR_Total_mmol_m2",
                 "BP_Average_kPa",
                 "AirTemp_C_Average",
                 "RH_percent",
                 "Rain_Total_mm",
                 "WindSpeed_Average_m_s",
                 "WindDir_degrees",
                 "ShortwaveRadiationUp_Average_W_m2",
                 "ShortwaveRadiationDown_Average_W_m2",
                 "InfraredRadiationUp_Average_W_m2",
                 "InfraredRadiationDown_Average_W_m2",
                 "Albedo_Average_W_m2",
                 "Flag_PAR_umolm2s_Average",
                 "Note_PAR_umolm2s_Average",
                 "Flag_PAR_Total_mmol_m2",
                 "Note_PAR_Total_mmol_m2",
                 "Flag_BP_Average_kPa",
                 "Note_BP_Average_kPa",
                 "Flag_AirTemp_C_Average",
                 "Note_AirTemp_C_Average",
                 "Flag_RH_percent",
                 "Note_RH_percent",
                 "Flag_Rain_Total_mm",
                 "Note_Rain_Total_mm",
                 "Flag_WindSpeed_Average_m_s",
                 "Note_WindSpeed_Average_m_s",
                 "Flag_WindDir_degrees",
                 "Note_WindDir_degrees",
                 "Flag_ShortwaveRadiationUp_Average_W_m2",
                 "Note_ShortwaveRadiationUp_Average_W_m2",
                 "Flag_ShortwaveRadiationDown_Average_W_m2",
                 "Note_ShortwaveRadiationDown_Average_W_m2",
                 "Flag_InfraredRadiationUp_Average_W_m2",
                 "Note_InfraredRadiationUp_Average_W_m2",
                 "Flag_InfraredRadiationDown_Average_W_m2",
                 "Note_InfraredRadiationDown_Average_W_m2",
                 "Flag_Albedo_Average_W_m2",
                 "Note_Albedo_Average_W_m2"    ), check.names=TRUE)

unlink(infile1)

# Fix any interval or ratio columns mistakenly read in as nominal and nominal columns read as numeric or dates read as strings

if (class(dt1$Reservoir)!="factor") dt1$Reservoir<- as.factor(dt1$Reservoir)
if (class(dt1$Site)=="factor") dt1$Site <-as.numeric(levels(dt1$Site))[as.integer(dt1$Site) ]
if (class(dt1$Site)=="character") dt1$Site <-as.numeric(dt1$Site)
# attempting to convert dt1$DateTime dateTime string to R date structure (date or POSIXct)
tmpDateFormat<-"%Y-%m-%d %H:%M:%S"
tmp1DateTime<-as.POSIXct(dt1$DateTime,format=tmpDateFormat)
# Keep the new dates only if they all converted correctly
if(length(tmp1DateTime) == length(tmp1DateTime[!is.na(tmp1DateTime)])){dt1$DateTime <- tmp1DateTime } else {print("Date conversion failed for dt1$DateTime. Please inspect the data and do the date conversion yourself.")}
rm(tmpDateFormat,tmp1DateTime)
if (class(dt1$Record)=="factor") dt1$Record <-as.numeric(levels(dt1$Record))[as.integer(dt1$Record) ]
if (class(dt1$Record)=="character") dt1$Record <-as.numeric(dt1$Record)
if (class(dt1$CR3000Battery_V)=="factor") dt1$CR3000Battery_V <-as.numeric(levels(dt1$CR3000Battery_V))[as.integer(dt1$CR3000Battery_V) ]
if (class(dt1$CR3000Battery_V)=="character") dt1$CR3000Battery_V <-as.numeric(dt1$CR3000Battery_V)
if (class(dt1$CR3000Panel_Temp_C)=="factor") dt1$CR3000Panel_Temp_C <-as.numeric(levels(dt1$CR3000Panel_Temp_C))[as.integer(dt1$CR3000Panel_Temp_C) ]
if (class(dt1$CR3000Panel_Temp_C)=="character") dt1$CR3000Panel_Temp_C <-as.numeric(dt1$CR3000Panel_Temp_C)
if (class(dt1$PAR_umolm2s_Average)=="factor") dt1$PAR_umolm2s_Average <-as.numeric(levels(dt1$PAR_umolm2s_Average))[as.integer(dt1$PAR_umolm2s_Average) ]
if (class(dt1$PAR_umolm2s_Average)=="character") dt1$PAR_umolm2s_Average <-as.numeric(dt1$PAR_umolm2s_Average)
if (class(dt1$PAR_Total_mmol_m2)=="factor") dt1$PAR_Total_mmol_m2 <-as.numeric(levels(dt1$PAR_Total_mmol_m2))[as.integer(dt1$PAR_Total_mmol_m2) ]
if (class(dt1$PAR_Total_mmol_m2)=="character") dt1$PAR_Total_mmol_m2 <-as.numeric(dt1$PAR_Total_mmol_m2)
if (class(dt1$BP_Average_kPa)=="factor") dt1$BP_Average_kPa <-as.numeric(levels(dt1$BP_Average_kPa))[as.integer(dt1$BP_Average_kPa) ]
if (class(dt1$BP_Average_kPa)=="character") dt1$BP_Average_kPa <-as.numeric(dt1$BP_Average_kPa)
if (class(dt1$AirTemp_C_Average)=="factor") dt1$AirTemp_C_Average <-as.numeric(levels(dt1$AirTemp_C_Average))[as.integer(dt1$AirTemp_C_Average) ]
if (class(dt1$AirTemp_C_Average)=="character") dt1$AirTemp_C_Average <-as.numeric(dt1$AirTemp_C_Average)
if (class(dt1$RH_percent)=="factor") dt1$RH_percent <-as.numeric(levels(dt1$RH_percent))[as.integer(dt1$RH_percent) ]
if (class(dt1$RH_percent)=="character") dt1$RH_percent <-as.numeric(dt1$RH_percent)
if (class(dt1$Rain_Total_mm)=="factor") dt1$Rain_Total_mm <-as.numeric(levels(dt1$Rain_Total_mm))[as.integer(dt1$Rain_Total_mm) ]
if (class(dt1$Rain_Total_mm)=="character") dt1$Rain_Total_mm <-as.numeric(dt1$Rain_Total_mm)
if (class(dt1$WindSpeed_Average_m_s)=="factor") dt1$WindSpeed_Average_m_s <-as.numeric(levels(dt1$WindSpeed_Average_m_s))[as.integer(dt1$WindSpeed_Average_m_s) ]
if (class(dt1$WindSpeed_Average_m_s)=="character") dt1$WindSpeed_Average_m_s <-as.numeric(dt1$WindSpeed_Average_m_s)
if (class(dt1$WindDir_degrees)=="factor") dt1$WindDir_degrees <-as.numeric(levels(dt1$WindDir_degrees))[as.integer(dt1$WindDir_degrees) ]
if (class(dt1$WindDir_degrees)=="character") dt1$WindDir_degrees <-as.numeric(dt1$WindDir_degrees)
if (class(dt1$ShortwaveRadiationUp_Average_W_m2)=="factor") dt1$ShortwaveRadiationUp_Average_W_m2 <-as.numeric(levels(dt1$ShortwaveRadiationUp_Average_W_m2))[as.integer(dt1$ShortwaveRadiationUp_Average_W_m2) ]
if (class(dt1$ShortwaveRadiationUp_Average_W_m2)=="character") dt1$ShortwaveRadiationUp_Average_W_m2 <-as.numeric(dt1$ShortwaveRadiationUp_Average_W_m2)
if (class(dt1$ShortwaveRadiationDown_Average_W_m2)=="factor") dt1$ShortwaveRadiationDown_Average_W_m2 <-as.numeric(levels(dt1$ShortwaveRadiationDown_Average_W_m2))[as.integer(dt1$ShortwaveRadiationDown_Average_W_m2) ]
if (class(dt1$ShortwaveRadiationDown_Average_W_m2)=="character") dt1$ShortwaveRadiationDown_Average_W_m2 <-as.numeric(dt1$ShortwaveRadiationDown_Average_W_m2)
if (class(dt1$InfraredRadiationUp_Average_W_m2)=="factor") dt1$InfraredRadiationUp_Average_W_m2 <-as.numeric(levels(dt1$InfraredRadiationUp_Average_W_m2))[as.integer(dt1$InfraredRadiationUp_Average_W_m2) ]
if (class(dt1$InfraredRadiationUp_Average_W_m2)=="character") dt1$InfraredRadiationUp_Average_W_m2 <-as.numeric(dt1$InfraredRadiationUp_Average_W_m2)
if (class(dt1$InfraredRadiationDown_Average_W_m2)=="factor") dt1$InfraredRadiationDown_Average_W_m2 <-as.numeric(levels(dt1$InfraredRadiationDown_Average_W_m2))[as.integer(dt1$InfraredRadiationDown_Average_W_m2) ]
if (class(dt1$InfraredRadiationDown_Average_W_m2)=="character") dt1$InfraredRadiationDown_Average_W_m2 <-as.numeric(dt1$InfraredRadiationDown_Average_W_m2)
if (class(dt1$Albedo_Average_W_m2)=="factor") dt1$Albedo_Average_W_m2 <-as.numeric(levels(dt1$Albedo_Average_W_m2))[as.integer(dt1$Albedo_Average_W_m2) ]
if (class(dt1$Albedo_Average_W_m2)=="character") dt1$Albedo_Average_W_m2 <-as.numeric(dt1$Albedo_Average_W_m2)
if (class(dt1$Flag_PAR_umolm2s_Average)!="factor") dt1$Flag_PAR_umolm2s_Average<- as.factor(dt1$Flag_PAR_umolm2s_Average)
if (class(dt1$Note_PAR_umolm2s_Average)!="factor") dt1$Note_PAR_umolm2s_Average<- as.factor(dt1$Note_PAR_umolm2s_Average)
if (class(dt1$Flag_PAR_Total_mmol_m2)!="factor") dt1$Flag_PAR_Total_mmol_m2<- as.factor(dt1$Flag_PAR_Total_mmol_m2)
if (class(dt1$Note_PAR_Total_mmol_m2)!="factor") dt1$Note_PAR_Total_mmol_m2<- as.factor(dt1$Note_PAR_Total_mmol_m2)
if (class(dt1$Flag_BP_Average_kPa)!="factor") dt1$Flag_BP_Average_kPa<- as.factor(dt1$Flag_BP_Average_kPa)
if (class(dt1$Note_BP_Average_kPa)!="factor") dt1$Note_BP_Average_kPa<- as.factor(dt1$Note_BP_Average_kPa)
if (class(dt1$Flag_AirTemp_C_Average)!="factor") dt1$Flag_AirTemp_C_Average<- as.factor(dt1$Flag_AirTemp_C_Average)
if (class(dt1$Note_AirTemp_C_Average)!="factor") dt1$Note_AirTemp_C_Average<- as.factor(dt1$Note_AirTemp_C_Average)
if (class(dt1$Flag_RH_percent)!="factor") dt1$Flag_RH_percent<- as.factor(dt1$Flag_RH_percent)
if (class(dt1$Note_RH_percent)!="factor") dt1$Note_RH_percent<- as.factor(dt1$Note_RH_percent)
if (class(dt1$Flag_Rain_Total_mm)!="factor") dt1$Flag_Rain_Total_mm<- as.factor(dt1$Flag_Rain_Total_mm)
if (class(dt1$Note_Rain_Total_mm)!="factor") dt1$Note_Rain_Total_mm<- as.factor(dt1$Note_Rain_Total_mm)
if (class(dt1$Flag_WindSpeed_Average_m_s)!="factor") dt1$Flag_WindSpeed_Average_m_s<- as.factor(dt1$Flag_WindSpeed_Average_m_s)
if (class(dt1$Note_WindSpeed_Average_m_s)!="factor") dt1$Note_WindSpeed_Average_m_s<- as.factor(dt1$Note_WindSpeed_Average_m_s)
if (class(dt1$Flag_WindDir_degrees)!="factor") dt1$Flag_WindDir_degrees<- as.factor(dt1$Flag_WindDir_degrees)
if (class(dt1$Note_WindDir_degrees)!="factor") dt1$Note_WindDir_degrees<- as.factor(dt1$Note_WindDir_degrees)
if (class(dt1$Flag_ShortwaveRadiationUp_Average_W_m2)!="factor") dt1$Flag_ShortwaveRadiationUp_Average_W_m2<- as.factor(dt1$Flag_ShortwaveRadiationUp_Average_W_m2)
if (class(dt1$Note_ShortwaveRadiationUp_Average_W_m2)!="factor") dt1$Note_ShortwaveRadiationUp_Average_W_m2<- as.factor(dt1$Note_ShortwaveRadiationUp_Average_W_m2)
if (class(dt1$Flag_ShortwaveRadiationDown_Average_W_m2)!="factor") dt1$Flag_ShortwaveRadiationDown_Average_W_m2<- as.factor(dt1$Flag_ShortwaveRadiationDown_Average_W_m2)
if (class(dt1$Note_ShortwaveRadiationDown_Average_W_m2)!="factor") dt1$Note_ShortwaveRadiationDown_Average_W_m2<- as.factor(dt1$Note_ShortwaveRadiationDown_Average_W_m2)
if (class(dt1$Flag_InfraredRadiationUp_Average_W_m2)!="factor") dt1$Flag_InfraredRadiationUp_Average_W_m2<- as.factor(dt1$Flag_InfraredRadiationUp_Average_W_m2)
if (class(dt1$Note_InfraredRadiationUp_Average_W_m2)!="factor") dt1$Note_InfraredRadiationUp_Average_W_m2<- as.factor(dt1$Note_InfraredRadiationUp_Average_W_m2)
if (class(dt1$Flag_InfraredRadiationDown_Average_W_m2)!="factor") dt1$Flag_InfraredRadiationDown_Average_W_m2<- as.factor(dt1$Flag_InfraredRadiationDown_Average_W_m2)
if (class(dt1$Note_InfraredRadiationDown_Average_W_m2)!="factor") dt1$Note_InfraredRadiationDown_Average_W_m2<- as.factor(dt1$Note_InfraredRadiationDown_Average_W_m2)
if (class(dt1$Flag_Albedo_Average_W_m2)!="factor") dt1$Flag_Albedo_Average_W_m2<- as.factor(dt1$Flag_Albedo_Average_W_m2)
if (class(dt1$Note_Albedo_Average_W_m2)!="factor") dt1$Note_Albedo_Average_W_m2<- as.factor(dt1$Note_Albedo_Average_W_m2)

# Convert Missing Values to NA for non-dates

dt1$Reservoir <- as.factor(ifelse((trimws(as.character(dt1$Reservoir))==trimws("NA")),NA,as.character(dt1$Reservoir)))
dt1$Site <- ifelse((trimws(as.character(dt1$Site))==trimws("NA")),NA,dt1$Site)
suppressWarnings(dt1$Site <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$Site))==as.character(as.numeric("NA"))),NA,dt1$Site))
dt1$Record <- ifelse((trimws(as.character(dt1$Record))==trimws("NA")),NA,dt1$Record)
suppressWarnings(dt1$Record <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$Record))==as.character(as.numeric("NA"))),NA,dt1$Record))
dt1$CR3000Battery_V <- ifelse((trimws(as.character(dt1$CR3000Battery_V))==trimws("NA")),NA,dt1$CR3000Battery_V)
suppressWarnings(dt1$CR3000Battery_V <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$CR3000Battery_V))==as.character(as.numeric("NA"))),NA,dt1$CR3000Battery_V))
dt1$CR3000Panel_Temp_C <- ifelse((trimws(as.character(dt1$CR3000Panel_Temp_C))==trimws("NA")),NA,dt1$CR3000Panel_Temp_C)
suppressWarnings(dt1$CR3000Panel_Temp_C <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$CR3000Panel_Temp_C))==as.character(as.numeric("NA"))),NA,dt1$CR3000Panel_Temp_C))
dt1$PAR_umolm2s_Average <- ifelse((trimws(as.character(dt1$PAR_umolm2s_Average))==trimws("NA")),NA,dt1$PAR_umolm2s_Average)
suppressWarnings(dt1$PAR_umolm2s_Average <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$PAR_umolm2s_Average))==as.character(as.numeric("NA"))),NA,dt1$PAR_umolm2s_Average))
dt1$PAR_Total_mmol_m2 <- ifelse((trimws(as.character(dt1$PAR_Total_mmol_m2))==trimws("NA")),NA,dt1$PAR_Total_mmol_m2)
suppressWarnings(dt1$PAR_Total_mmol_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$PAR_Total_mmol_m2))==as.character(as.numeric("NA"))),NA,dt1$PAR_Total_mmol_m2))
dt1$BP_Average_kPa <- ifelse((trimws(as.character(dt1$BP_Average_kPa))==trimws("NA")),NA,dt1$BP_Average_kPa)
suppressWarnings(dt1$BP_Average_kPa <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$BP_Average_kPa))==as.character(as.numeric("NA"))),NA,dt1$BP_Average_kPa))
dt1$AirTemp_C_Average <- ifelse((trimws(as.character(dt1$AirTemp_C_Average))==trimws("NA")),NA,dt1$AirTemp_C_Average)
suppressWarnings(dt1$AirTemp_C_Average <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$AirTemp_C_Average))==as.character(as.numeric("NA"))),NA,dt1$AirTemp_C_Average))
dt1$RH_percent <- ifelse((trimws(as.character(dt1$RH_percent))==trimws("NA")),NA,dt1$RH_percent)
suppressWarnings(dt1$RH_percent <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$RH_percent))==as.character(as.numeric("NA"))),NA,dt1$RH_percent))
dt1$Rain_Total_mm <- ifelse((trimws(as.character(dt1$Rain_Total_mm))==trimws("NA")),NA,dt1$Rain_Total_mm)
suppressWarnings(dt1$Rain_Total_mm <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$Rain_Total_mm))==as.character(as.numeric("NA"))),NA,dt1$Rain_Total_mm))
dt1$WindSpeed_Average_m_s <- ifelse((trimws(as.character(dt1$WindSpeed_Average_m_s))==trimws("NA")),NA,dt1$WindSpeed_Average_m_s)
suppressWarnings(dt1$WindSpeed_Average_m_s <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$WindSpeed_Average_m_s))==as.character(as.numeric("NA"))),NA,dt1$WindSpeed_Average_m_s))
dt1$WindDir_degrees <- ifelse((trimws(as.character(dt1$WindDir_degrees))==trimws("NA")),NA,dt1$WindDir_degrees)
suppressWarnings(dt1$WindDir_degrees <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$WindDir_degrees))==as.character(as.numeric("NA"))),NA,dt1$WindDir_degrees))
dt1$ShortwaveRadiationUp_Average_W_m2 <- ifelse((trimws(as.character(dt1$ShortwaveRadiationUp_Average_W_m2))==trimws("NA")),NA,dt1$ShortwaveRadiationUp_Average_W_m2)
suppressWarnings(dt1$ShortwaveRadiationUp_Average_W_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$ShortwaveRadiationUp_Average_W_m2))==as.character(as.numeric("NA"))),NA,dt1$ShortwaveRadiationUp_Average_W_m2))
dt1$ShortwaveRadiationDown_Average_W_m2 <- ifelse((trimws(as.character(dt1$ShortwaveRadiationDown_Average_W_m2))==trimws("NA")),NA,dt1$ShortwaveRadiationDown_Average_W_m2)
suppressWarnings(dt1$ShortwaveRadiationDown_Average_W_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$ShortwaveRadiationDown_Average_W_m2))==as.character(as.numeric("NA"))),NA,dt1$ShortwaveRadiationDown_Average_W_m2))
dt1$InfraredRadiationUp_Average_W_m2 <- ifelse((trimws(as.character(dt1$InfraredRadiationUp_Average_W_m2))==trimws("NA")),NA,dt1$InfraredRadiationUp_Average_W_m2)
suppressWarnings(dt1$InfraredRadiationUp_Average_W_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$InfraredRadiationUp_Average_W_m2))==as.character(as.numeric("NA"))),NA,dt1$InfraredRadiationUp_Average_W_m2))
dt1$InfraredRadiationDown_Average_W_m2 <- ifelse((trimws(as.character(dt1$InfraredRadiationDown_Average_W_m2))==trimws("NA")),NA,dt1$InfraredRadiationDown_Average_W_m2)
suppressWarnings(dt1$InfraredRadiationDown_Average_W_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$InfraredRadiationDown_Average_W_m2))==as.character(as.numeric("NA"))),NA,dt1$InfraredRadiationDown_Average_W_m2))
dt1$Albedo_Average_W_m2 <- ifelse((trimws(as.character(dt1$Albedo_Average_W_m2))==trimws("NA")),NA,dt1$Albedo_Average_W_m2)
suppressWarnings(dt1$Albedo_Average_W_m2 <- ifelse(!is.na(as.numeric("NA")) & (trimws(as.character(dt1$Albedo_Average_W_m2))==as.character(as.numeric("NA"))),NA,dt1$Albedo_Average_W_m2))
dt1$Flag_PAR_umolm2s_Average <- as.factor(ifelse((trimws(as.character(dt1$Flag_PAR_umolm2s_Average))==trimws("NA")),NA,as.character(dt1$Flag_PAR_umolm2s_Average)))
dt1$Note_PAR_umolm2s_Average <- as.factor(ifelse((trimws(as.character(dt1$Note_PAR_umolm2s_Average))==trimws("NA")),NA,as.character(dt1$Note_PAR_umolm2s_Average)))
dt1$Flag_PAR_Total_mmol_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_PAR_Total_mmol_m2))==trimws("NA")),NA,as.character(dt1$Flag_PAR_Total_mmol_m2)))
dt1$Note_PAR_Total_mmol_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_PAR_Total_mmol_m2))==trimws("NA")),NA,as.character(dt1$Note_PAR_Total_mmol_m2)))
dt1$Flag_BP_Average_kPa <- as.factor(ifelse((trimws(as.character(dt1$Flag_BP_Average_kPa))==trimws("NA")),NA,as.character(dt1$Flag_BP_Average_kPa)))
dt1$Note_BP_Average_kPa <- as.factor(ifelse((trimws(as.character(dt1$Note_BP_Average_kPa))==trimws("NA")),NA,as.character(dt1$Note_BP_Average_kPa)))
dt1$Flag_AirTemp_C_Average <- as.factor(ifelse((trimws(as.character(dt1$Flag_AirTemp_C_Average))==trimws("NA")),NA,as.character(dt1$Flag_AirTemp_C_Average)))
dt1$Note_AirTemp_C_Average <- as.factor(ifelse((trimws(as.character(dt1$Note_AirTemp_C_Average))==trimws("NA")),NA,as.character(dt1$Note_AirTemp_C_Average)))
dt1$Flag_RH_percent <- as.factor(ifelse((trimws(as.character(dt1$Flag_RH_percent))==trimws("NA")),NA,as.character(dt1$Flag_RH_percent)))
dt1$Note_RH_percent <- as.factor(ifelse((trimws(as.character(dt1$Note_RH_percent))==trimws("NA")),NA,as.character(dt1$Note_RH_percent)))
dt1$Flag_Rain_Total_mm <- as.factor(ifelse((trimws(as.character(dt1$Flag_Rain_Total_mm))==trimws("NA")),NA,as.character(dt1$Flag_Rain_Total_mm)))
dt1$Note_Rain_Total_mm <- as.factor(ifelse((trimws(as.character(dt1$Note_Rain_Total_mm))==trimws("NA")),NA,as.character(dt1$Note_Rain_Total_mm)))
dt1$Flag_WindSpeed_Average_m_s <- as.factor(ifelse((trimws(as.character(dt1$Flag_WindSpeed_Average_m_s))==trimws("NA")),NA,as.character(dt1$Flag_WindSpeed_Average_m_s)))
dt1$Note_WindSpeed_Average_m_s <- as.factor(ifelse((trimws(as.character(dt1$Note_WindSpeed_Average_m_s))==trimws("NA")),NA,as.character(dt1$Note_WindSpeed_Average_m_s)))
dt1$Flag_WindDir_degrees <- as.factor(ifelse((trimws(as.character(dt1$Flag_WindDir_degrees))==trimws("NA")),NA,as.character(dt1$Flag_WindDir_degrees)))
dt1$Note_WindDir_degrees <- as.factor(ifelse((trimws(as.character(dt1$Note_WindDir_degrees))==trimws("NA")),NA,as.character(dt1$Note_WindDir_degrees)))
dt1$Flag_ShortwaveRadiationUp_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_ShortwaveRadiationUp_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Flag_ShortwaveRadiationUp_Average_W_m2)))
dt1$Note_ShortwaveRadiationUp_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_ShortwaveRadiationUp_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Note_ShortwaveRadiationUp_Average_W_m2)))
dt1$Flag_ShortwaveRadiationDown_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_ShortwaveRadiationDown_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Flag_ShortwaveRadiationDown_Average_W_m2)))
dt1$Note_ShortwaveRadiationDown_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_ShortwaveRadiationDown_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Note_ShortwaveRadiationDown_Average_W_m2)))
dt1$Flag_InfraredRadiationUp_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_InfraredRadiationUp_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Flag_InfraredRadiationUp_Average_W_m2)))
dt1$Note_InfraredRadiationUp_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_InfraredRadiationUp_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Note_InfraredRadiationUp_Average_W_m2)))
dt1$Flag_InfraredRadiationDown_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_InfraredRadiationDown_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Flag_InfraredRadiationDown_Average_W_m2)))
dt1$Note_InfraredRadiationDown_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_InfraredRadiationDown_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Note_InfraredRadiationDown_Average_W_m2)))
dt1$Flag_Albedo_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Flag_Albedo_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Flag_Albedo_Average_W_m2)))
dt1$Note_Albedo_Average_W_m2 <- as.factor(ifelse((trimws(as.character(dt1$Note_Albedo_Average_W_m2))==trimws("NA")),NA,as.character(dt1$Note_Albedo_Average_W_m2)))



## read in "new" met data from github
dt_new <- read.csv("https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-metstation-data/FCRmet_L1.csv")


df_met_full <- rbind(dt1,dt_new)


## find indicies for every 7th day
doy[doy == 1 | doy %% 7 == 0]




## Create forecasts from historical data
library(zoo)
library(smooth)
library(lubridate)

df_date_col_select <- df_met_full %>%
  mutate(doy = as.numeric(strftime(DateTime, format = "%j"))) %>%
  mutate(date = format(as.Date(DateTime))) %>%
  mutate(hour = hour(DateTime)) %>%
  select(-contains('Note')) %>%
  select(-contains('Flag')) %>%
  select(-c('Reservoir','Site','Albedo_Average_W_m2','CR3000Battery_V','CR3000Panel_Temp_C','PAR_umolm2s_Average','PAR_Total_mmol_m2','InfraredRadiationUp_Average_W_m2'))
  #select()
  # group_by(date) %>%
  # mutate(avg_temp = mean(AirTemp_C_Average)) %>%
  # ungroup() %>%
  # distinct(date, .keep_all = TRUE) %>%
  # select(date,time,doy, avg_temp)

rm(df_met_full)


df_hourly_avgs <- df_date_col_select %>%
  group_by(date,hour) %>%
  mutate(UGRD = mean(WindSpeed_Average_m_s*cos(WindDir_degrees),na.rm=TRUE)) %>%
  mutate(VGRD = mean(WindSpeed_Average_m_s*sin(WindDir_degrees),na.rm=TRUE)) %>%
  mutate(PRES = mean(BP_Average_kPa*1000,na.rm=TRUE)) %>%
  mutate(TMP = mean(AirTemp_C_Average, na.rm =TRUE)) %>%
  mutate(RH = mean(RH_percent, na.rm=TRUE)) %>%
  mutate(APCP = mean(Rain_Total_mm, na.rm=TRUE)) %>%
  mutate(DLWRF = mean(InfraredRadiationDown_Average_W_m2, na.rm=TRUE)) %>%
  mutate(DSWRF = mean(ShortwaveRadiationDown_Average_W_m2,na.rm=TRUE)) %>%
  ungroup() %>%
  distinct(date, hour, .keep_all = TRUE) %>%
  select(date,hour,doy,UGRD,VGRD,PRES,TMP,RH,APCP,DLWRF,DSWRF)

df_met_long <- df_hourly_avgs %>%
  gather(key = 'variable', value = 'prediction', -doy, -date, -hour) %>%
  mutate(year = format(as.Date(date, format="%Y-%m-%d"),"%Y")) %>%
  filter(date > '2015-12-31') %>%
  filter(date < '2023-01-01')

df_met_long$parameter <- NA
df_met_long$parameter <- ifelse(df_met_long$year == '2016',1,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2017',2,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2018',3,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2019',4,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2020',5,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2021',6,df_met_long$parameter)
df_met_long$parameter <- ifelse(df_met_long$year == '2022',7,df_met_long$parameter)


## ADD EXTRA COLUMNS
df_met_long$site_id <- 'fcre'
df_met_long$family <- 'ensemble'
df_met_long$longitude <- '-79.83722'
df_met_long$latitude <- '37.30315'

df_met_long$height <- NA
df_met_long$height <- ifelse(df_met_long$variable == 'UGRD','10 m above ground',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'VGRD','10 m above ground',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'PRES','surface',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'TMP','2 m above ground',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'RH','2 m above ground',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'APCP','surface',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'DLWRF','surface',df_met_long$height)
df_met_long$height <- ifelse(df_met_long$variable == 'DSWRF','surface',df_met_long$height)


date_store <- df_met_long %>%
  mutate(month_day = format(as.Date(date, format="%Y-%m-%d"),"%m-%d")) %>%
  select(month_day,hour,doy) %>%
  distinct(doy, hour, .keep_all = TRUE)


df_met_join <- date_store %>%
  right_join(df_met_long, by=c('doy','hour'))

## make list of doy values choose start times
reference_date_list <- date_store[date_store$doy ==1 | date_store$doy %% 7 ==0,'doy'][[1]]

# #save every year with rolling avg
# df_2016 <- df_met_full %>% filter(date < '2017-01-01', date > '2015-12-31') %>% select(doy,avg_temp) %>%
#   #mutate(avg_temp_5ma = sma(avg_temp,n=5)) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2016 = avg_temp_5ra) %>%
#   select(doy,temp_2016)
#
#
# df_2017 <- df_airtemp %>% filter(date < '2018-01-01', date > '2016-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2017 = avg_temp_5ra) %>%
#   select(doy,temp_2017)
#
# df_2018 <- df_airtemp %>% filter(date < '2019-01-01', date > '2017-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2018 = avg_temp_5ra) %>%
#   select(doy,temp_2018)
#
#
# df_2019 <- df_airtemp %>% filter(date < '2020-01-01', date > '2018-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2019 = avg_temp_5ra) %>%
#   select(doy,temp_2019)
#
#
# df_2020 <- df_airtemp %>% filter(date < '2021-01-01', date > '2019-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2020 = avg_temp_5ra) %>%
#   select(doy,temp_2020)
#
#
# df_2021 <- df_airtemp %>% filter(date < '2022-01-01', date > '2020-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2021 = avg_temp_5ra) %>%
#   select(doy,temp_2021)
#
#
# df_2022 <- df_airtemp %>% filter(date < '2023-01-01', date > '2021-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2022 = avg_temp_5ra) %>%
#   select(doy,temp_2022)
#
#
# df_all_temp <- df_2020 %>%
#   left_join(df_2021, by = 'doy') %>%
#   left_join(df_2022, by = 'doy') %>%
#   left_join(df_2019, by = 'doy') %>%
#   left_join(df_2018, by = 'doy') %>%
#   left_join(df_2017, by = 'doy') %>%
#   left_join(df_2016, by = 'doy')
#




# ## Plot airtemp data
# library(zoo)
# library(smooth)
#
# df_airtemp <- dt1 %>%
#   mutate(doy = as.numeric(strftime(DateTime, format = "%j"))) %>%
#   mutate(date = format(as.Date(DateTime))) %>%
#   mutate(time = format(as.POSIXct(DateTime), format = "%H:%M")) %>%
#   group_by(date) %>%
#   mutate(avg_temp = mean(AirTemp_C_Average)) %>%
#   ungroup() %>%
#   distinct(date, .keep_all = TRUE) %>%
#   select(date,time,doy, avg_temp)
#
#
# #save every year with rolling avg
# df_2016 <- df_airtemp %>% filter(date < '2017-01-01', date > '2015-12-31') %>% select(doy,avg_temp) %>%
#   #mutate(avg_temp_5ma = sma(avg_temp,n=5)) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2016 = avg_temp_5ra) %>%
#   select(doy,temp_2016)
#
#
# df_2017 <- df_airtemp %>% filter(date < '2018-01-01', date > '2016-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2017 = avg_temp_5ra) %>%
#   select(doy,temp_2017)
#
# df_2018 <- df_airtemp %>% filter(date < '2019-01-01', date > '2017-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2018 = avg_temp_5ra) %>%
#   select(doy,temp_2018)
#
#
# df_2019 <- df_airtemp %>% filter(date < '2020-01-01', date > '2018-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2019 = avg_temp_5ra) %>%
#   select(doy,temp_2019)
#
#
# df_2020 <- df_airtemp %>% filter(date < '2021-01-01', date > '2019-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2020 = avg_temp_5ra) %>%
#   select(doy,temp_2020)
#
#
# df_2021 <- df_airtemp %>% filter(date < '2022-01-01', date > '2020-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2021 = avg_temp_5ra) %>%
#   select(doy,temp_2021)
#
#
# df_2022 <- df_airtemp %>% filter(date < '2023-01-01', date > '2021-12-31') %>% select(doy,avg_temp) %>%
#   mutate(avg_temp_5ra = zoo::rollmean(avg_temp,k=5,fill = NA)) %>%
#   rename(temp_2022 = avg_temp_5ra) %>%
#   select(doy,temp_2022)
#
#
# df_all_temp <- df_2020 %>%
#   left_join(df_2021, by = 'doy') %>%
#   left_join(df_2022, by = 'doy') %>%
#   left_join(df_2019, by = 'doy') %>%
#   left_join(df_2018, by = 'doy') %>%
#   left_join(df_2017, by = 'doy') %>%
#   left_join(df_2016, by = 'doy')
#
#
# ### LOOK AT PRECIP DATA
# df_precip <- dt1 %>%
#   #mutate(doy = as.numeric(strftime(DateTime, format = "%j"))) %>%
#   #mutate(date = format(as.Date(DateTime))) %>%
#   #mutate(time = format(as.POSIXct(DateTime), format = "%H:%M")) %>%
#   mutate(year = format(as.Date(DateTime, format="%d/%m/%Y"),"%Y")) %>%
#   group_by(year) %>%
#   mutate(total_annual_precip_mm = sum(Rain_Total_mm, na.rm = TRUE)) %>%
#   ungroup() %>%
#   distinct(year,total_annual_precip)
#   #mutate(avg_precip = mean(Rain_Total_mm)) %>%
#   #ungroup()
#   #distinct(date, .keep_all = TRUE) %>%
#   #select(date,time,doy, avg_precip)

#df_long <- gather(df_all_temp,key='year',value = 'airtemp', -doy)

# ggplot(df_long, aes(x=doy, y=airtemp, group=year, color=year)) +
#   geom_line() +
#   scale_color_manual(values = c("temp_2016" = 'darkred',
#                                 'temp_2017' = 'steelblue',
#                                 'temp_2018' = 'goldenrod4',
#                                 'temp_2019' = 'springgreen4',
#                                 'temp_2020' = 'chocolate2',
#                                 'temp_2021' = 'grey25',
#                                 'temp_2022' = 'black')) +
#   scale_shape_manual(values = c("temp_2016" = 0,
#                                 'temp_2017' = 1,
#                                 'temp_2018' = 2,
#                                 'temp_2019' = 5,
#                                 'temp_2020' = 10,
#                                 'temp_2021' = 7,
#                                 'temp_2022' = 9)) +
#   ggtitle('FCRE AirTemp (C)') +
#   xlab('DOY') +
#   ylab('Air Temp (C)')


# ggplot(df_all_temp, aes(x=doy)) +
#   geom_line(aes(y = temp_2016), color = "darkred") +
#   geom_line(aes(y = temp_2017), color="steelblue", linetype="twodash") +
#   geom_line(aes(y = temp_2018), color="goldenrod4", linetype="F1") +
#   geom_line(aes(y = temp_2019), color="springgreen4", linetype="longdash") +
#   geom_line(aes(y = temp_2020), color="chocolate2", linetype="dotdash") +
#   geom_line(aes(y = temp_2021), color="grey25", linetype="solid") +
#   geom_line(aes(y = temp_2022), color="black", linetype="dotted")


# ggplot(df_all_temp, aes(x=doy)) +
#   geom_point(aes(y = temp_2016), color = "darkred") +
#   geom_point(aes(y = temp_2017), color="steelblue", shape=0) +
#   geom_point(aes(y = temp_2018), color="goldenrod4", shape=1) +
#   geom_point(aes(y = temp_2019), color="springgreen4", shape=2) +
#   geom_point(aes(y = temp_2020), color="chocolate2", shape = 5) +
#   geom_point(aes(y = temp_2021), color="grey25", shape=7) +
#   geom_point(aes(y = temp_2022), color="black", shape=9) +
#   labs(x = "DOY",
#        y = "AirTemp (C)",
#        color = "Legend")
