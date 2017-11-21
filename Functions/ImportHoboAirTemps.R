##############################################################################################################################
#     Title: StageToDischarge
#     Description: This script will process raw HOBO data files containing stage and
#     temperature data by calculating discharge using rating coefficients from the Wachusett Hydro Database (tblRatings).
#     Output:
#     Written by: Dan Crocker, July 2017
##############################################################################################################################

# LOAD REQUIRED LIBRARIES
library(RODBC)
library(odbc)
library(DBI)
library(tidyverse)
library(data.table)
library(lubridate)
library(openxlsx)
library(DescTools)
library(scales)
library(extrafont)
library(readxl) # new to swap code that gets the data to read_excel()
#################
# INITIAL SETUP #
#################
# Convert DateTime format and update timezone

# GET THE HOBO DATA #
# Directory with HOBO stage data:
  folder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs"
# Make a list of all the files in the folder

  files <- grep(
    x = list.files(folder, ignore.case = T, include.dirs = F),
    pattern = "^(?=.*\\b.xlsx\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
    value = T,
    perl =T)
  files
  # Select the file to calculate discharge
  file <- files[1]
  # Get the full path to the file
  path <- paste0(folder,"/", file)
# Read in the data to a dataframe
  
 sd <- read_excel(path, sheet= 1, col_names = T, trim_ws = T) %>% 
    as.data.frame()
# START PROCESSING THE DATA #
 HOBO_PROCESS <- function(){
# Remove first row which contains useless column headers
  sd <- sd[-1,1:2]
# Change column names
  names(sd) = c("DateTime", "AirTemp_F")
  sd <- sd[!sd$DateTime == "",]
# Change data types to numeric
  sd[sapply(sd, is.character)] <- lapply(sd[sapply(sd, is.character)], as.numeric)
# Convert DateTime format and update timezone
  sd$DateTime <- XLDateToPOSIXct(sd$DateTime)
  sd$DateTime <- force_tz(sd$DateTime, tzone = "America/New_York")

# Get Location Name
  trib <- substr(path,53,56)

################### MAKE COPY OF DATA TO USE GOING FORWARD

# Create new dataframe that calculates daily min, mean, max
  sd1 <- sd %>%
    group_by(date(sd$DateTime)) %>%
    summarize(AirTemp_F_min = min(AirTemp_F), AirTemp_F_mean = mean(AirTemp_F), AirTemp_F_max = max(AirTemp_F))

# Change first column name to "Date"
  colnames(sd1)[1] <- "DATE"
# Assign the appropriate trib code
  sd1$TRIBUTARY <- trib
# Generate a new ID number for the rating
  con <- dbConnect(odbc::odbc(),
                   .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                              paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                   timezone = "America/New_York")
  qry.maxID <- dbGetQuery(con,"SELECT max(ID) FROM tblHOBO_AIRTEMP")
  # Get current max ID
  if(is.na(qry.maxID)) {
    qry.maxID <- 0
  } else {
    qry.maxID <- qry.maxID
  }
  maxID <- as.numeric(unlist(qry.maxID))
  rm(qry.maxID)

  sd1$ID <- seq.int(nrow(sd1)) + maxID
  #Close the db connection and remove the connection
  dbDisconnect(con)
  rm(con)
  # Make sure all column headers are uppercase and then rearrange columns and set data type to data frame
  names(sd1) <- toupper(names(sd1))
  sd1 <-  sd1[, c(6,5,1, 2:4)]
  sd1 <- as.data.frame(sd1)

 } # END HOBO_PROCESS - STOP HERE AND INSPECT DATA BEFORE IMPORTING TO WQ DATABASE
sd1 <- HOBO_PROCESS() # Run the function to process the stage/water temp data

# INSPECT DATA TO MAKE SURE IT LOOKS GOOD BEFORE IMPORTING TO WQDB
#
odbcCloseAll()
con <-  odbcConnectAccess(filename.db)
# Save the discharge and temp data to the table in the WQDatabase
  sqlSave(con, sd1, tablename = "tblHOBO_AIRTEMP", append = TRUE,
          rownames = FALSE, colnames = FALSE, addPK = FALSE , fast = F)
# Move the processed hobo file to the processed folder
  file.rename(path, paste0(folder,"/Processed_HOBO_data/",file))
#Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)

summary(sd1)

###########################
#         PLOTTING        #
###########################
con <-  odbcConnectAccess(filename.db)
loc <- "MD83"
hobo <- sqlFetch(con, "tblHOBO_AIRTEMP")
summary(hobo)
pd <- filter(hobo, TRIBUTARY == loc)
# # Reshape data for plotting
# pd1 <- pd %>%
#   dplyr::select(c(3,5,8)) %>%
#   gather(key = msmt, value, 2:3)

title <- paste0("Air Temperature from HOBOs\n At Tributary ", loc)
y1lim <- max(pd$AIR_TEMP_F_MAX)

  plot  <- ggplot(pd, aes(x = as.Date(DATE))) +
    geom_line(aes(y = pd$AIR_TEMP_F_MIN, color = "Daily Min Temp (F)"), size = 0.25)  +
    geom_line(aes(y = pd$AIR_TEMP_F_MEAN, color = "Daily Mean Temp (F)"), size = 1)  +
    geom_line(aes(y = pd$AIR_TEMP_F_MAX, color = "Daily Max Temp (F)"), size = 0.25)  +
    geom_point(aes(y = pd$AIR_TEMP_F_MIN, color = "Daily Min Temp (F)"), size = 0.5)  +
    geom_point(aes(y = pd$AIR_TEMP_F_MEAN, color = "Daily Mean Temp (F)"), size = 1.25)  +
    geom_point(aes(y = pd$AIR_TEMP_F_MAX, color = "Daily Max Temp (F)"), size = 0.5)  +
    scale_y_continuous(breaks = pretty_breaks(n=6),limits = c(0,y1lim)) +
    scale_x_date(breaks = date_breaks("months"),
                 labels = date_format("%b\n%Y")) +
    scale_colour_manual(values = c("lightcoral", "darkblue", "darkseagreen4")) +
    labs(y = "Air Temperature (F)",
         x = "Date",
         color = "") +
    ggtitle(title) +
    theme(plot.title = element_text(family = "Arial",color= "black", face="bold", size=14, vjust = 1, hjust = 0.5),
          legend.position = "bottom",
        axis.title.x = element_text(angle = 0, face = "bold", color = "black"),
        axis.title.y = element_text(angle = 90, face = "bold", color = "black"))

plot

# Export the plot - This can eventually be turned into a function and tied to an action button in Shiny
path <- paste0(folder,"/Plots")
now <- format(Sys.time(), "%Y%m%d")
path <- paste0(path, "/StageDischarge_", "at_", loc, "-", now, ".png")
png(filename=path, units="in", width=9.5, height=7, res=300)
print(plot)
dev.off()
