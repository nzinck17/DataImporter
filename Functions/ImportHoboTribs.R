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

# GET THE HOBO DATA #

# Directory with HOBO stage data:
  folder <- "W:/WatershedJAH/EQStaff/WQDatabase/TribStages_HOBOs"
# Make a list of all the files in the folder
  files <- list.files(folder, include.dirs = TRUE)
  files
  # Select the file to calculate discharge
  file <- files[2]
  # Get the full path to the file
  path <- paste0(folder,"/", file)
# Read in the data to a dataframe
  sd <- openxlsx::read.xlsx(path, sheet = 1, colNames = TRUE, cols = c(1,3,5)) # This is the raw stage data
# START PROCESSING THE DATA #
 HOBO_PROCESS <- function(){
# Remove first row which contains useless column headers
  sd <- sd[-1,]
# Change column names
  names(sd) = c("DateTime", "WaterTemp_F", "Stage_ft")
  sd <- sd[!sd$DateTime == "",]
# Change data types to numeric
  sd[sapply(sd, is.character)] <- lapply(sd[sapply(sd, is.character)], as.numeric)
# Convert DateTime format and update timezone
  sd$DateTime <- XLDateToPOSIXct(sd$DateTime)
  sd$DateTime <- force_tz(sd$DateTime, tzone = "America/New_York")

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
  sd1 <- sd
###################

# Add three new columns to hold ratingNo, rating part, and calculated discharge
  sd1$ratingNo <- 0
  sd1$part <- 0
  sd1$q_cfs <- 0


######################################
# BEGIN LOOPING THROUGH STAGE VALUES #
######################################

# For each stage value assign the appropriate rating number based on the date ranges of the ratings
# x is the each date value

  x <- sd1$DateTime
  sd1$ratingNo <- sapply(x, function(x) ratings2$ID[ratings2$Start <= x & ratings2$End >= x])
  sd1$ratingNo <-  as.numeric(sd1$ratingNo)

  ### Old Method - sapply much faster
  # for (i in seq_along()) {
  #   sd1$ratingNo[i] <- ratings2$ID[ratings2$Start <= sd1$Date[i] & ratings2$End >= sd1$Date[i]]
  # }

  #################################################################
# Assign the rating part to each stage
x <- sd1$ratingNo
y <- sd1$Stage_ft

part <- function(x,y) {
if(ratings2$Parts[ratings2$ID == x] == 1) {# Rating has 1 part
  1
  } else if(ratings2$Parts[ratings2$ID == x] == 2) { # Rating has 2 parts
      if(y < ratings2$Break1[ratings2$ID == x]) {# stage is less than breakpoint 1
      1
    } else 2 # Otherwise stage is >= breakpoint1
    # If no return yet, then the rating has 3 parts
  } else {
    if(y[sd1$ratingNo == x] < ratings2$Break1[ratings2$ID == x]) { # stage is less than breakpoint 1
      1 # The stage is in the first part of the rating
      } else if(y[sd1$ratingNo == x] >= ratings2$Break2[ratings2$ID == x]) { # stage is higher than breakpoint 2
          3
        } else 2
  }
}

sd1$part <- mapply(part,x,y) %>% as.numeric()
#sd1$part <- as.numeric(sd1$part)

#######################################################################

### Assign the rating part to each stage - OLD WAY - SUPERCEDED BY ABOVE
  # for (i in seq_along(sd1$ratingNo)) {
  #   if(ratings2$Parts[ratings2$ID == sd1$ratingNo[i]] == 1) { # It is a 1 part Rating
  #       sd1$part <- 1 # The rating has 1 part - assign part 1
  #     } else { # rating has 2 or 3 parts - so there is at least a #Breakpoint 1
  #       parts <- ifelse(is.na(ratings2$Break2[ratings2$ID == sd1$ratingNo[i]]), 2, 3)
  #       if(parts == 2) { # This is a 2 part rating
  #       # If the stage is less than Breakpoint 1 then use the first part of rating
  #         if(sd1$Stage_ft[i] < ratings2$Break1[ratings2$ID == sd1$ratingNo[i]]) {
  #           sd1$part[i] <- 1 # The stage is in the first part of the rating - assign part 1
  #         } else {
  #           sd1$part[i] <- 2 # The stage is above the first breakpoint - assign part 2
  #         }
  #         next
  #       }
  #     }
  #         if(parts == 3) { # This is a 3 part rating
  #             if(sd1$Stage_ft[i] < ratings2$Break1[ratings2$ID == sd1$ratingNo[i]]) {
  #               sd1$part[i] <- 1 # The stage is in the first part of the rating - assign part 1
  #               } else {
  #                 if(sd1$Stage_ft[i] >= ratings2$Break2[ratings2$ID == sd1$ratingNo[i]]) {
  #                 sd1$part[i] <- 3 # Stage is above Break2 - assign part 3
  #                 } else {
  #                   sd1$part[i] <- 2 # Rating must be in part 2 since it is not in part 1 or part 3
  #                 }
  #                 next
  #               }
  #         }
  #   }

# LOOP THROUGH ALL STAGE VALUES AND CALCULATE THE DISCHARGE USING RATING COEFFICIENTS

  for (i in seq_along(sd1$q_cfs)) {
    minstage <- ratings2$MinStage[ratings2$ID == sd1$ratingNo[i]]
    maxstage <- ratings2$MaxStage[ratings2$ID == sd1$ratingNo[i]]

    if(sd1$Stage_ft < minstage | sd1$Stage_ft > maxstage) {
      # Then stage < min_stage or > max stage - then q <- -99 skip calculation and go to next
      sd1$q_cfs[i] <- -99
      next
    } else {
      # Rating Coefficients part 1
      c1 <- ratings2$C1[ratings2$ID == sd1$ratingNo[i]]
      a1 <- ratings2$a1[ratings2$ID == sd1$ratingNo[i]]
      n1 <- ratings2$n1[ratings2$ID == sd1$ratingNo[i]]
      # Rating Coefficients part 2
      c2 <- ratings2$C2[ratings2$ID == sd1$ratingNo[i]]
      a2 <- ratings2$a2[ratings2$ID == sd1$ratingNo[i]]
      n2 <- ratings2$n2[ratings2$ID == sd1$ratingNo[i]]
      # Rating Coefficients part 3
      c3 <- ratings2$C3[ratings2$ID == sd1$ratingNo[i]]
      a3 <- ratings2$a3[ratings2$ID == sd1$ratingNo[i]]
      n3 <- ratings2$n3[ratings2$ID == sd1$ratingNo[i]]

      C <- paste0("c", sd1$part[i])
      a <- paste0("a", sd1$part[i])
      n <- paste0("n", sd1$part[i])
    }
    # Define function to find Q:
    findq <- function(stage, C, n, a) {
      C*(stage-a)^n
    }
    # Use findq function to calculate discharge from each stage
      sd1$q_cfs[i] <- findq(stage = sd1$Stage_ft[i], C = get(C), a = get(a), n = get(n))
  }

################### MAKE COPY OF DATA TO USE GOING FORWARD
  sd2 <- sd1
###################

# Create new dataframe that calculates daily min, mean, max
  sd2 <- sd2 %>%
    group_by(date(sd2$DateTime)) %>%
    summarize(stage_min = min(Stage_ft), stage_mean = mean(Stage_ft), stage_max = max(Stage_ft),
              q_min_cfs = min(q_cfs), q_mean_cfs = mean(q_cfs), q_max_cfs = max(q_cfs),
              WaterTemp_min = min(WaterTemp_F), WaterTemp_mean = mean(WaterTemp_F), WaterTemp_max = max(WaterTemp_F))

# Change first column name to "Date"
  colnames(sd2)[1] <- "DATE"
# Assign the appropriate trib code
  sd2$TRIBUTARY <- trib
# Generate a new ID number for the rating
  hobo <- sqlFetch(con, "tblHOBO_DATA")
  LastID <- as.numeric(max(hobo$ID))
  if(LastID == -Inf) {
    LastID <- 0
  } else {
    LastID <- LastID
  }
  sd2$ID <- seq.int(nrow(sd2)) + LastID
  #Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)
  # Make sure all column headers are uppercase and then rearrange columns and set data type to data frame
  names(sd2) <- toupper(names(sd2))
  sd2 <-  sd2[, c(12,11, 1:10)]
  sd2 <- as.data.frame(sd2)

 } # END HOBO_PROCESS - STOP HERE AND INSPECT DATA BEFORE IMPORTING TO WQ DATABASE
sd2 <- HOBO_PROCESS() # Run the function to process the stage/water temp data

#
# INSPECT DATA TO MAKE SURE IT LOOKS GOOD BEFORE IMPORTING TO WQDB
#
odbcCloseAll()
con <-  odbcConnectAccess("C:/WQDatabase/WaterQualityDB_fe.mdb")
# Save the discharge and temp data to the table in the WQDatabase
  sqlSave(con, sd2, tablename = "tblHOBO_DATA", append = TRUE,
          rownames = FALSE, colnames = FALSE, addPK = FALSE , fast = F)
# Move the processed hobo file to the processed folder
  file.rename(path, paste0(folder,"/Processed/",file))
#Close the db connection and remove the connection
  odbcCloseAll()
  rm(con)

summary(sd2)

###########################
#         PLOTTING        #
###########################
con <-  odbcConnectAccess("C:/WQDatabase/WaterQualityDB_fe.mdb")
loc <- "MD01"
hobo <- sqlFetch(con, "tblHOBO_DATA")
summary(hobo)
pd <- filter(hobo, TRIBUTARY == loc)
# # Reshape data for plotting
# pd1 <- pd %>%
#   dplyr::select(c(3,5,8)) %>%
#   gather(key = msmt, value, 2:3)

title <- paste0("Stage and Calculated Discharges from HOBOs\n At Tributary ", loc)
y1lim <- max(pd$Q_MEAN_CFS)
y2lim <- max(pd$STAGE_MEAN)
mult <- y1lim / y2lim

  plot  <- ggplot(pd, aes(x = DATE)) +
    geom_line(aes(y = pd$STAGE_MEAN * mult, color = "Daily Mean Stage (ft)"), size = 0.5)  +
    geom_line(aes(y = pd$Q_MEAN_CFS, color = "Daily Mean Discharge (cfs)"), size = 0.5) +
    geom_point(aes(y = pd$STAGE_MEAN * mult, color = "Daily Mean Stage (ft)"), size = 1)  +
    geom_point(aes(y = pd$Q_MEAN_CFS, color = "Daily Mean Discharge (cfs)"), size = 1) +
    scale_y_continuous(breaks = pretty_breaks(),limits = c(0,y1lim),
                       sec.axis = sec_axis(~./mult, breaks = pretty_breaks(), name = "Stage (ft)")) +
    scale_colour_manual(values = c("blue", "firebrick4")) +
    labs(y = "Discharge (Cubic Feet per Second)",
         x = "Date",
         colour = "") +
    ggtitle(title) +
    theme(plot.title = element_text(family = "Arial",color= "black", face="bold", size=14, vjust = 1, hjust = 0.5),
        legend.position = c(0.2, 0.6),
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
