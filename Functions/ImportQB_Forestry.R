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
library(readxl)
library(lubridate)


##########################  # NOTE - THIS TOP SECTION IS FOR TESTING THE FUNCTION OUTSIDE SHINY  
# READ NEW RAW DATA FILE #  # COMMENT OUT SECTION BELOW WHEN RUNNING FUNCTION IN SHINY
##########################

### Specify Data type and set the raw data folder ###

datatype <- "Tributary/Transect" # Options are Tributary or Reservoir, Forestry (other?)

# Generate Function Arguments
# Specify the raw data folder based on data type:
if (datatype == "Tributary/Transect") {
  rawdatafolder <- "W:/WatershedJAH/EQStaff/WQDatabase/MWRA_Data/MWRA_RawDataUnprocessed" # This folder is set up and good to use
  processedfolder <- "W:/WatershedJAH/EQStaff/WQDatabase/MWRA_Data/MWRA_RawData_ImportedToDB"
  filename.db <- "DBQ=C:/WQDatabase/WACHUSETT_WQDB_fe.mdb"
} else {
  rawdatafolder <- "W:/WatershedJAH/EQStaff/Aquatic Biology/AB database/MWRA_Data/MWRA_RawDataUnprocessed" # This needs to be created/confirmed
  processedfolder <- "W:/WatershedJAH/EQStaff/Aquatic Biology/AB database/MWRA_Data/MWRA_RawData_ImportedToDB"
  filename.db <- "DBQ=C:/WQDatabase/WACHUSETT_AQBIO_DB_fe.mdb"
}
#filename.db <- "DBQ=C:/WQDatabase/QUABBIN_WQDB_fe.mdb"
filename.db <- "DBQ=W:/WatershedJAH/DataTransfer/QB_FORESTRY_WQDB.mdb" #### TEMPORARY _ DELETE LATER
### Add paths for other data types as necessary

### Find and set the file to import ###

files <- grep(
  x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
  pattern = "^(?=.*\\bxlsx\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
  value = T,
  perl =T
)

#  Make a list of xlsx files in raw data folder
files # Only for testing - This will print the list of contents in the folder - In shiny, use this list to populate a select input - user will select data file

# Select the file to import
file <- files[1] # this is manually set for testing, in Shiny the file would get set to input$file
#this could be set up to do multiple files at a time, but its safer to just do one at a time

#########################################################################################################

# COMMENT OUT ABOVE CODE EXCEPT FOR LOADING LIBRARIES WHEN RUNNING IN SHINY

#############################
#   PROCESSING FUNCTION    #
############################

PROCESS_DATA <- function(file, rawdatafolder, filename.db){ # Start the function - takes 1 input (File)
options(scipen = 50) # Eliminate Scientific notation in numerical fields
# Get the full path to the file
path <- paste0(rawdatafolder,"/", file)

# Read in the data to a dataframe
df.wq <- read_excel(path, col_names = T, trim_ws = T, na = "nil") %>% 
  as.data.frame()   # This is the raw data - data comes in as xlsx file, so read.csv will not work
df.wq <- df.wq[,c(1:25)]
### Perform Data checks ###

# At this point there could be a number of checks to make sure data is valid
  # Check to make sure there are 25 variables (columns)  
  if (ncol(df.wq) != 25) {
    # Send warning message to UI 
    #warning1 <- print(paste0("There are not 25 columns of data in this file.\n Check the file before proceeding"))
    stop("There are not 25 columns of data in this file.\n Check the file before proceeding")
  }
  
  # Check to make sure column 1 is "Original Sample" or other?
  if (any(colnames(df.wq)[1] != "Original Sample" & df.wq[25] != "X Ldl")) {
    # Send warning message to UI 
    #warning2 <- print(paste0("At least 1 column heading is unexpected.\n Check the file before proceeding"))
    stop("At least 1 column heading is unexpected.\n Check the file before proceeding")
  }
  
  # Check to see if there were any miscellaneous locations that did not get assigned a location
  if (length(which(str_detect(df.wq$Location, "MISC"),TRUE)) > 0) {
    #warning3 <- print(paste0("There are unspecified (MISC) locations that need to be corrected before importing data"))
    stop("There are unspecified (MISC) locations that need to be corrected before importing data")
  }
# Check to see if there were any GENERAL locations that did not get assigned a location
if (length(which(str_detect(df.wq$Location, "GENERAL-GEN"),TRUE)) > 0) {
  #warning3 <- print(paste0("There are unspecified (MISC) locations that need to be corrected before importing data"))
  stop("There are unspecified (GEN) locations that need to be corrected before importing data")
}
  #Dup check  - need to do this after UniqueID Column is created. See below...

# Any other checks?  Otherwise data is validated, proceed to reformatting...
### 

### Need to insert code here to abort function if any warnings are triggered


# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")
#################################
#  START REFORMATTING THE DATA  #
#################################

### Rename Columns in Raw Data    
names(df.wq) = c("SampleGroup",  
                 "SampleNumber",
                 "TextID",
                 "Location",
                 "Description",
                 "TripNum",
                 "LabRecDate",
                 "SampleDate",
                 "SampleTime",
                 "PrepOn",
                 "DateTimeAnalyzed",
                 "AnalyzedBy",
                 "Analysis",
                 "ReportedName",
                 "Parameter",
                 "ResultReported",
                 "Units",
                 "Comment",
                 "SampledBy",
                 "Status",
                 "EDEP_Confirm",
                 "EDEP_MW_Confirm",
                 "Reportable",
                 "Method",
                 "DetectionLimit")


### Date and Time: 
# SampleDateTime
# Split the Sample time into date and time
df.wq$SampleDate <- as.Date(df.wq$SampleDate)
#df.wq$SampleTime[is.na(df.wq$SampleTime)] <- paste(df.wq$SampleDate[is.na(df.wq$SampleTime)])

df.wq <- separate(df.wq, SampleTime, into = c("date", "Time"), sep = " ")

# Merge the actual date column with the new Time Column and reformat to POSIXct
df.wq$SampleDateTime <- as.POSIXct(paste(as.Date(df.wq$SampleDate, format ="%Y-%m-%d"), df.wq$Time, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)   
#df.wq$SampleDateTime[is.na(df.wq$Time)] <- as.POSIXct(paste(df.wq$SampleDate[is.na(df.wq$Time)]), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)  

# Fix other data types
df.wq$EDEP_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$EDEP_MW_Confirm <- as.character(df.wq$EDEP_Confirm)
df.wq$Comment <- as.character(df.wq$Comment)

# Fix the Parameter names  - change from MWRA name to ParameterName
params <- dbReadTable(con,"tblParameters")
df.wq$Parameter <- params$ParameterName[match(df.wq$Parameter, params$ParameterMWRAName)]

# Delete possible Sample Address rows (Associated with MISC Sample Locations):
df.wq <- filter(df.wq, !is.na(ResultReported)) # Filter out any sample with no results (There shouldn't be, but they do get included sometimes)

df.wq <- df.wq %>% slice(which(!grepl("Sample Address", df.wq$Parameter, fixed = TRUE)))
df.wq <- df.wq %>% slice(which(!grepl("(DEP)", df.wq$Parameter, fixed = TRUE))) # Filter out rows where Parameter contains  "(DEP)"
#df.wq <- df.wq %>% slice(which(!grepl("X", df.wq$Status, fixed = TRUE))) # Filter out records where Status is X
# Fix the Location names
df.wq$Location <- gsub("WACHUSET-","", df.wq$Location)
df.wq$Location <- gsub("BMP1","FPRN", df.wq$Location)
df.wq$Location <- gsub("BMP2","FHLN", df.wq$Location)
df.wq$Location <- gsub("QUABBINT-","", df.wq$Location)
df.wq$Location <- gsub("QUABBIN-","", df.wq$Location)

######################
#   Add new Columns  #
######################

### Unique ID number

df.wq$UniqueID <- paste(df.wq$Location, df.wq$SampleDateTime, params$ParameterAbbreviation[match(df.wq$Parameter, params$ParameterName)], sep = "_")

### Make sure it is unique within the data file - if not then exit function and send warning
# dupecheck <- which(duplicated(df.wq$UniqueID))
# dupes <- df.wq$UniqueID[dupecheck] # These are the dupes
# 
# if (length(dupes) > 0){
#   # Exit function and send a warning to userlength(dupes) # number of dupes
#   stop(paste0("This data file contains ", length(dupes),
#              " records that appear to be duplicates. Eliminate all duplicates before proceeding"))
#   print(dupes) # Show the duplicate Unique IDs to user in Shiny
# }
### Make sure records are not already in DB

Uniq <- dbGetQuery(con,"SELECT UniqueID FROM tblWQALLDATA")
dupes2 <- Uniq$UniqueID[Uniq$UniqueID %in% df.wq$UniqueID]

if (length(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste0("This data file contains ", length(dupes2), 
              " records that appear to already exist in the database! Eliminate all duplicates before proceeding"))
  #print(dupes2) # Show the duplicate Unique IDs to user in Shiny 
}
rm(Uniq)

### DataSource
df.wq <- df.wq %>% mutate(DataSource = paste0("MWRA_", file))
### DataSourceID
# Do some sorting first:
df.wq <- df.wq[with(df.wq, order(SampleDateTime, Location, Parameter)),]

# Assign the numbers
df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)

# note: some reported results are "A" for (DEP). These will be NA in the ResultFinal Columns
# ResultReported - 
# Find all valid results, change to numeric and round to 6 digits in order to eliminate scientific notation
# Replace those results with the updated value converted back to character 

edits <- str_detect(df.wq$ResultReported, paste(c("<",">"), collapse = '|')) %>% 
  which(T)
update <- as.numeric(df.wq$ResultReported[-edits], digits = 6)
df.wq$ResultReported[-edits] <- as.character(update)

### FinalResult (numeric)
# Make the variable
df.wq$FinalResult <- NA 
# Set the vector for mapply to operate on
x <- df.wq$ResultReported
# Function to determine FinalResult
FR <- function(x) {
  if(str_detect(x, "<")){# BDL
    as.numeric(gsub("<","", x), digits =4) * 0.5 # THEN strip "<" from reported result, make numeric, divide by 2. 
  } else if (str_detect(x, ">")){
    as.numeric(gsub(">","", x)) # THEN strip ">" form reported result, make numeric. 
  } else { 
    as.numeric(x)
  }# ELSE THEN just use Result Reported for Result and make numeric
}  
df.wq$FinalResult <- mapply(FR,x) %>% 
  round(digits = 4)
### Flag (numeric)
# Use similar function as to assign flags
df.wq$FlagCode <- NA
FLAG <- function(x) {
  if (str_detect(x, "<")) {
    100     # THEN set to 100 for BDL
  } else if (str_detect(x, ">")){
    101     # THEN set to 101 for ADL
  } else {
    NA
  } 
}
  df.wq$FlagCode <- mapply(FLAG,x) %>% as.numeric()

  ### Storm Sample (Actually logical data - yes/no, but Access stores as -1 for yes and 0 for no. 1 bit
  df.wq$StormSample <- 0 %>%  as.numeric() 

### Storm SampleN (numeric)
df.wq$StormSampleN <- NA %>% as.numeric

### Importdate (Date)
df.wq <- df.wq %>% mutate(ImportDate = Sys.Date())

#####################################################################

### IDs

# Read Tables
# WQ
query.wq <- dbGetQuery(con,"SELECT max(ID) FROM tblWQALLDATA")
# Get current max ID
if(is.na(query.wq)) {
  query.wq <- 0
} else {
  query.wq <- query.wq
}
ID.max.wq <- unlist(query.wq)
rm(query.wq)
### ID wq
df.wq$ID <- seq.int(nrow(df.wq)) + ID.max.wq

# Flags
query.flags <- dbGetQuery(con,"SELECT max(ID) FROM tblSampleFlagIndex") 
# Get current max ID
if(is.na(query.flags)) {
  query.flags <- 0
} else {
  query.flags <- query.flags
}
ID.max.flags <- unlist(query.flags)
rm(query.flags)

# Split the flags into a separate df and assign new ID

df.flags <- as.data.frame(select(df.wq,c("ID","FlagCode"))) %>% 
  rename("SampleFlag_ID" = ID) %>% 
  drop_na()

### ID flags
df.flags$ID <- seq.int(nrow(df.flags)) + ID.max.flags

##############################################################################################################################
# Reformatting 2
##############################################################################################################################

### Deselect Columns that do not need in Database
df.wq <- df.wq %>% select(-c(#Description, 
                             SampleDate,
                             date,
                             #flag, # Still exists in table, after records are moved for good, then delete here
                             Time
                             )
)

# Reorder remaining 32 columns to match the database table exactly
col.order.wq <- dbListFields(con, "tblWQALLDATA")
df.wq <-  df.wq[,col.order.wq]


# Reorder df.flags columns to match the database table exactly
df.flags <- df.flags[,c(3,1,2)]

# Create a list of the processed datasets
dfs <- list()
dfs[[1]] <- df.wq
dfs[[2]] <- df.flags
dfs[[3]] <- path
# Disconnect from db and remove connection obj
dbDisconnect(con)
rm(con)
return(dfs)
} # END FUNCTION

#### COMMENT OUT WHEN RUNNING SHINY

#RUN THE FUNCTION TO PROCESS THE DATA AND RETURN 2 DATAFRAMES and path AS LIST:
dfs <- PROCESS_DATA(file, rawdatafolder, filename.db)

# Extract each element needed
df.wq     <- dfs[[1]]
df.flags  <- dfs[[2]]
path      <- dfs[[3]]

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags, path, file, filename.db){
  # Connect to db 
  #filename.db <- "C:/WQDatabase/WaterQualityDB_fe.mdb" # This needs to point to the front end database

  con <-  odbcConnectAccess(filename.db)
  
  # Import the data to the database - Need to use RODBC methods here. Tried odbc and it failed
  # WQ Data
  ColumnsOfTable <- sqlColumns(con, "tblWQALLDATA")
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME) 
  sqlSave(con, df.wq, tablename = "tblWQALLDATA", append = T, 
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  
  # Flag data
  sqlSave(con, df.flags, tablename = "tblSampleFlagIndex", append = T,
          rownames = F, colnames = F, addPK = F , fast = F)
  
  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
  
  # Move the processed raw data file to the processed folder
  # processed_subdir <- paste0("/", max(year(df.wq$SampleDateTime))) # Raw data archived by year, subfolders = Year
  # processed_dir <- paste0(processedfolder, processed_subdir)
  # file.rename(path, paste0(processed_dir,"/", file))
}
IMPORT_DATA(df.wq, df.flags, path, file, filename.db)  
### END 
