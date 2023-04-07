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

edi_file <- dt1

source('R/edi_qaqc_combine.R')
library(tidyverse)
library(lubridate)
library(yaml)
library(EDIutils)




## read offset file
entity_names <- read_data_entity_names(packageId = 'edi.389.7')
met_edi <- read_data_entity(packageId = 'edi.389.7', entityId = entity_names$entityId[1])
met_edi <- readr::read_csv(file = met_edi)

L1_file <- read.csv('https://raw.githubusercontent.com/addelany/FCRE-data/fcre-metstation-data/FCRmet_L1.csv')

test_df <- met_data_bind(realtime_file = L1_file, qaqc_file = edi_file, input_file_tz = "America/New_York")
