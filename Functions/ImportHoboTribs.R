##############################################################################################################################
#     Title: ImportHOBOTribs.R
#     Description: This script will process raw HOBO data files containing stage and
#     temperature data by calculating discharge using rating coefficients from the Wachusett Hydro Database (tblRatings).
#     Output: df.wq - Daily flow/water temp records imported to WQDB
#     Written by: Dan Crocker, July 2017, revised November 2017
##############################################################################################################################

# LOAD REQUIRED LIBRARIES
# library(RODBC)
# library(odbc)
# library(DBI)
# library(tidyverse)
# library(data.table)
# library(lubridate)
# library(DescTools)
# library(scales)
# library(readxl) # new to swap code that gets the data to read_excel()

#################
# INITIAL SETUP #
#################

### Function Arguments:
# 
# rawdatafolder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs"
# processedfolder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs/Processed_HOBO_data"
# filename.db <- "C:/WQDatabase/WaterQualityDB_fe.mdb"
# 
# # Make a list of all the files in the folder
#   files <- grep(
#     x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
#     pattern = "^(?=.*\\b(xlsx|xlsm)\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
#     value = T,
#     perl =T)
#   files
#   # Select the file to calculate discharge
#   file <- files[1]
  # Get the full path to the file
  
# START PROCESSING THE DATA #
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL){
   
   path <- paste0(rawdatafolder, "/", file)
   # Read in the data to a dataframe
   df.wq <- read_excel(path, sheet = 1, col_names = TRUE, trim_ws = TRUE, range = cell_cols("A:E")) # This is the raw stage data

# Remove first row which contains useless column headers and only keep columns 1,3,5
  df.wq <- df.wq[-1,c(1,3,5)]
# Change column names
  names(df.wq) = c("DateTime", "WaterTemp_F", "Stage_ft")
  df.wq <- df.wq[!df.wq$DateTime == "",]
# Change data types to numeric
  df.wq[sapply(df.wq, is.character)] <- lapply(df.wq[sapply(df.wq, is.character)], as.numeric)
# Convert DateTime format and update timezone
  df.wq$DateTime <- XLDateToPOSIXct(df.wq$DateTime)
  df.wq$DateTime <- force_tz(df.wq$DateTime, tzone = "America/New_York")

###########################################
# GET RATING INFORMATION FROM WQ DATABASE #
###########################################

# Set odbc connection  and get the rating table
  con <-  odbcConnectAccess("C:/WQDatabase/WaterQualityDB_fe.mdb")
  ratings <- sqlFetch(con, "tblRatings")
# Assigntoday's date as the end date for ratings that are still valid - so that date test won't compare against NA values
  now <- format(Sys.time(), "%Y-%m-%d")
  ratings$End[is.na(ratings$End)] <- now
# Get the tributary code for this stage data
  trib <- substr(path,53,56)
# Extract ratings for current trib
  ratings2 <- ratings[ratings$MWRA_Loc == trib,]
# how many ratings are there?
  #NumRatings <- as.numeric(length(ratings2$ID))

################### MAKE COPY OF DATA TO USE GOING FORWARD
 # df.wq <- sd
###################

# Add three new columns to hold ratingNo, rating part, and calculated discharge
  df.wq$ratingNo <- 0
  df.wq$part <- 0
  df.wq$q_cfs <- 0

######################################
# BEGIN LOOPING THROUGH STAGE VALUES #
######################################

# For each stage value assign the appropriate rating number based on the date ranges of the ratings
# x is the each date value

  x <- df.wq$DateTime
  df.wq$ratingNo <- sapply(x, function(x) ratings2$ID[ratings2$Start <= x & ratings2$End >= x])
  df.wq$ratingNo <-  as.numeric(df.wq$ratingNo)


#################################################################
# Assign the rating part to each stage
x <- df.wq$ratingNo
y <- df.wq$Stage_ft

part <- function(x,y) {
if(ratings2$Parts[ratings2$ID == x] == 1) {# Rating has 1 part
  1
  } else if(ratings2$Parts[ratings2$ID == x] == 2) { # Rating has 2 parts
      if(y < ratings2$Break1[ratings2$ID == x]) {# stage is less than breakpoint 1
      1
    } else 2 # Otherwise stage is >= breakpoint1
    # If no return yet, then the rating has 3 parts
  } else {
    if(y[df.wq$ratingNo == x] < ratings2$Break1[ratings2$ID == x]) { # stage is less than breakpoint 1
      1 # The stage is in the first part of the rating
      } else if(y[df.wq$ratingNo == x] >= ratings2$Break2[ratings2$ID == x]) { # stage is higher than breakpoint 2
          3
        } else 2
  }
}

df.wq$part <- mapply(part,x,y) %>% as.numeric()

# LOOP THROUGH ALL STAGE VALUES AND CALCULATE THE DISCHARGE USING RATING COEFFICIENTS

  for (i in seq_along(df.wq$q_cfs)) {
    minstage <- ratings2$MinStage[ratings2$ID == df.wq$ratingNo[i]]
    maxstage <- ratings2$MaxStage[ratings2$ID == df.wq$ratingNo[i]]

    if(df.wq$Stage_ft < minstage | df.wq$Stage_ft > maxstage) {
      # Then stage < min_stage or > max stage - then q <- -99 skip calculation and go to next
      df.wq$q_cfs[i] <- -99
      next
    } else {
      # Rating Coefficients part 1
      c1 <- ratings2$C1[ratings2$ID == df.wq$ratingNo[i]]
      a1 <- ratings2$a1[ratings2$ID == df.wq$ratingNo[i]]
      n1 <- ratings2$n1[ratings2$ID == df.wq$ratingNo[i]]
      # Rating Coefficients part 2
      c2 <- ratings2$C2[ratings2$ID == df.wq$ratingNo[i]]
      a2 <- ratings2$a2[ratings2$ID == df.wq$ratingNo[i]]
      n2 <- ratings2$n2[ratings2$ID == df.wq$ratingNo[i]]
      # Rating Coefficients part 3
      c3 <- ratings2$C3[ratings2$ID == df.wq$ratingNo[i]]
      a3 <- ratings2$a3[ratings2$ID == df.wq$ratingNo[i]]
      n3 <- ratings2$n3[ratings2$ID == df.wq$ratingNo[i]]

      C <- paste0("c", df.wq$part[i])
      a <- paste0("a", df.wq$part[i])
      n <- paste0("n", df.wq$part[i])
    }
    # Define function to find Q:
    findq <- function(stage, C, n, a) {
      C*(stage-a)^n
    }
    # Use findq function to calculate discharge from each stage
      df.wq$q_cfs[i] <- findq(stage = df.wq$Stage_ft[i], C = get(C), a = get(a), n = get(n))
  }

# Create new dataframe that calculates daily min, mean, max
  df.wq <- df.wq %>%
    group_by(date(df.wq$DateTime)) %>%
    summarize(stage_min = min(Stage_ft), stage_mean = mean(Stage_ft), stage_max = max(Stage_ft),
              q_min_cfs = min(q_cfs), q_mean_cfs = mean(q_cfs), q_max_cfs = max(q_cfs),
              WaterTemp_min = min(WaterTemp_F), WaterTemp_mean = mean(WaterTemp_F), WaterTemp_max = max(WaterTemp_F))

# Change first column name to "Date"
  colnames(df.wq)[1] <- "DATE"
# Assign the appropriate trib code
  df.wq$TRIBUTARY <- trib
# Generate a new ID number for the rating
  hobo <- sqlFetch(con, "tblHOBO_DATA")
  LastID <- as.numeric(max(hobo$ID))
  if(LastID == -Inf) {
    LastID <- 0
  } else {
    LastID <- LastID
  }
  df.wq$ID <- seq.int(nrow(df.wq)) + LastID
  #Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
  # Make sure all column headers are uppercase and then rearrange columns and set data type to data frame
  names(df.wq) <- toupper(names(df.wq))
  df.wq <-  df.wq[, c(12,11, 1:10)]
  df.wq <- as.data.frame(df.wq)
  return(df.wq)
 } # PROCESS_DATA - STOP HERE AND INSPECT DATA BEFORE IMPORTING TO WQ DATABASE
#df.wq <- PROCESS_DATA(file, rawdatafolder, filename.db) # Run the function to process the stage/water temp data

#
# INSPECT DATA TO MAKE SURE IT LOOKS GOOD BEFORE IMPORTING TO WQDB
#

##############
#IMPORT DATA #
##############
 IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder){

# processedfolder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs/Processed_HOBO_data"    
   
# Connect to db using RODBC
   con <-  odbcConnectAccess(filename.db)
# Save the discharge and temp data to the table in the WQDatabase
  sqlSave(con, df.wq, tablename = "tblHOBO_DATA", append = TRUE,
          rownames = FALSE, colnames = FALSE, addPK = FALSE , fast = F)
# Move the processed hobo file to the processed folder
  file.rename(path, paste0(processedfolder,"/",file))
#Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
 }
