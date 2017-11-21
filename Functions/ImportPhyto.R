##############################################################################################################################
#     Title: FormatMWRA.R
#     Description: This script will Format/Process MWRA data to DCR 
#     Written by: Nick Zinck/Dan Crocker, October, 2017
#     Note: Dplyr mutate is used a lot in this script. base R [] could also be used.
#
#    This script will process and import MWRA Projects: WATTRB, WATTRN, MDCMNTH, WATBMP, QUABBIN, MDHEML
#     - As of 10/23/17 testing results positive for WATTRB/WATTRN
#     - Edits to script will likely be needed after testing other project data
#     - Additional variables may need to be generated to interact with shiny App
##############################################################################################################################

# Load libraries needed
library(tidyverse)
library(stringr)
library(odbc)
library(RODBC)
library(DBI)
library(lubridate)
library(magrittr)
library(readxl)
library(DescTools)
library(lubridate)


# Tidyverse and readxl are loaded in App.r
# NOTE - THIS TOP SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY
# COMMENT OUT SECTION BELOW WHEN RUNNING FUNCTION IN SHINY

######################################################################################################################    
# READ NEW RAW DATA FILE ############################################################################################# 
###################################################################################################################### 

 ### Function Arguments:

rawdatafolder <- paste0("W:/WatershedJAH/EQStaff/Aquatic Biology/Plankton/", year(Sys.Date())," Wachusett Algae")
processedfolder <- paste0("W:/WatershedJAH/EQStaff/Aquatic Biology/Plankton/", year(Sys.Date())," Wachusett Algae")
filename.db <- "C:/WQDatabase/AqBioDBWachusett_fe.mdb"

# ### Find the file to Import
# files <- grep(
#   x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
#   pattern = "^(?=.*\\b(xlsx|xlsm)\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
#   value = T,
#   perl =T
# )
# 
# 
# #  List the files:
# files
# 
# # Select the file to import manually
# file <- files[3]
file <- paste0("Zeiss20x_", year(Sys.Date()), "_test.xlsx")
######################################################################################################################
######################################################################################################################
######################################################################################################################

# COMMENT OUT ABOVE CODE EXCEPT FOR LOADING LIBRARIES WHEN RUNNING IN SHINY

#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL){ # Start the function - takes 1 input (File)
options(scipen = 50) # Eliminate Scientific notation in numerical fields
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the data to a dataframe
df.wq <- read_excel(path, sheet= 2, col_names = F, trim_ws = T, range = "AM6:AV112") %>% 
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
# Eliminate blank rows
df.wq <- df.wq[!is.na(df.wq[1]),]
# Temporarily rename columns
names(df.wq) <- c(1:10)

# Split off Density dataset and reformat:

##################
# PHYTO DENSITY #
#################

df.phyto <- t(df.wq[,c(1:5)]) %>% # Transpose columns 1-5
  as.data.frame() 
# Use first row as column names
colnames(df.phyto) <- as.character(unlist(df.phyto[1,]))
df.phyto = df.phyto[-1, ] # Delete row 1 and rename rows
rownames(df.phyto) <- seq(length=nrow(df.phyto)) 
df.phyto <- filter(df.phyto, Location != "0") %>% #
  gather("Taxa","Density",8:82, na.rm =F)

# Change to numeric
df.phyto$Date <- as.numeric(levels(df.phyto$Date))[df.phyto$Date]
df.phyto$`Depth (m)` <- as.numeric(levels(df.phyto$`Depth (m)`))[df.phyto$`Depth (m)`]
df.phyto$Magnification <- as.numeric(levels(df.phyto$Magnification))[df.phyto$Magnification]
df.phyto$Density <- round(as.numeric(df.phyto$Density)) # Round to integer
# Change to factor
df.phyto$Taxa <- as.factor(df.phyto$Taxa)
# Change date format
df.phyto$Date <- XLDateToPOSIXct(df.phyto$Date)
df.phyto$Date <- force_tz(df.phyto$Date, tzone = "America/New_York")

##################### 
# PRESENCE ABSENCE #
####################

df.PA <- t(df.wq[,c(1,7:10)]) %>% 
  as.data.frame()
# Use first row as column names
colnames(df.PA) <- as.character(unlist(df.PA[1,]))
df.PA = df.PA[-1, ] # Delete row 1 and rename rows
rownames(df.PA) <- seq(length=nrow(df.PA)) 
df.PA <- filter(df.PA, Location != "0") %>% #
  gather("Taxa","PA_Value",8:82, na.rm =F)

# Bring PA Value to df.phyto
df.phyto$PA_Value <- as.numeric(df.PA$PA_Value)
rm(df.PA)
rm(df.wq)
# Rename columns
### Rename Columns in Raw Data    
names(df.phyto) = c("Phyt_Date",  
                 "Phyt_Station",
                 "Phyt_Depth_m",
                 "Analyst",
                 "Microscope",
                 "Magnification",
                 "Method",
                 "Taxa",
                 "Density",
                 "PA_Value")

###################### 
# ADD NEW VARIABLES #
#####################

### Importdate (Date)
df.phyto$ImportDate <- Sys.Date() %>% as.Date()
### Unique ID 
df.phyto$UniqueID <- paste(df.phyto$Phyt_Station, df.phyto$Phyt_Date, df.phyto$Taxa, paste0(df.phyto$Phyt_Depth_m,"m"), sep = "_")

### Data Source
df.phyto$DataSource <- paste0(file)

# Assign the Data Source ID
df.phyto$DataSourceID <- seq(1, nrow(df.phyto), 1)

# Any data checks?  Otherwise data is validated, proceed to reformatting...
### 


# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")



## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df.phyto$UniqueID))
dupes <- df.phyto$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste0("This data file contains ", length(dupes),
             " records that appear to be duplicates. Eliminate all duplicates before proceeding"))
  #print(dupes) # Show the duplicate Unique IDs to user in Shiny
}
### Make sure records are not already in DB

Uniq <- dbGetQuery(con,"SELECT UniqueID FROM tbl_Phyto")
dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df.phyto$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2), 
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding"))
  #print(dupes2) # Show the duplicate Unique IDs to user in Shiny 
}
rm(Uniq)


# Read Tables
query.phyto <- dbGetQuery(con,"SELECT max(Phyt_ID) FROM tbl_Phyto")
# Get current max Phyt_ID
if(is.na(query.phyto)) {
  query.phyto <- 0
} else {
  query.phyto <- query.phyto
}
ID.max.phyto <- as.numeric(unlist(query.phyto))
rm(query.phyto)

### ID phyto
df.phyto$Phyt_ID <- seq.int(nrow(df.phyto)) + ID.max.phyto

##############################################################################################################################
# Reformatting 2
##############################################################################################################################

# Reorder remaining 32 columns to match the database table exactly
col.order.phyto <- dbListFields(con, "tbl_PhytoNEW")
df.phyto <-  df.phyto[,col.order.phyto]

# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(df.phyto)
} # END FUNCTION

#### COMMENT OUT WHEN RUNNING SHINY
########################################################################################################
#RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
df.phyto <- PROCESS_DATA(file, rawdatafolder, filename.db)

########################################################################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.phyto, filename.db, df.flags = NULL){
  
  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed
  con <-  odbcConnectAccess(filename.db)
  
  # WQ Data
  ColumnsOfTable <- sqlColumns(con, "tbl_PhytoNEW")
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME) 
  sqlSave(con, df.phyto, tablename = "tbl_PhytoNEW", append = T, 
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)

  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
  
}
### END 

IMPORT_DATA(df.phyto, filename.db)
