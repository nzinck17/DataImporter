 ##############################################################################################################################
#     Title: App.R
#     Type: Master file DCR Inport Data Shiny App
#     Description: This Shiny App contains the "master" script for the Import Data app. The app contains a ui and server component
#           and sources R scripts from the App folder
#     Written by: Nick Zinck, Spring 2017
##############################################################################################################################

# Notes:
#   1.
#
# To-Do List:
#   1.
 
####################################################################################################
# Load Libraries and Script (Sources, Modules, and Functions)
#####################################################################################################
 
 #setwd("//env-fp-wby-301.env.govt.state.ma.us/home$/DCrocker/DOCUMENTS/R/MWRA_Data_Importer")
 
 ### Load packages
 
 ipak <- function(pkg){
   new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
   if (length(new.pkg))
     install.packages(new.pkg, dependencies = TRUE, repos="http://cran.rstudio.com/")
   sapply(pkg, require, character.only = TRUE)
 }
 packages <- c("shinyjs", "shinythemes", "readxl", "tidyverse")
 ipak(packages)

 
 # WD should be location of App.R... if not set it below
setwd("//env-fp-wby-301.env.govt.state.ma.us/home$/DCrocker/DOCUMENTS/R/Data_Importer")
 
 ### Load table of datasets
datasets <-  read_excel("C:/WQDatabase/DataImporter/datasets.xlsx", sheet = 1, col_names = T, trim_ws = T) %>%
  filter(ImportMethod == "Importer-R")

###################################################################################
##################################  User Interface  ###############################
###################################################################################

ui <- fluidPage(theme = shinytheme("slate"),
  
fluidRow(br(), br(), br(), br(), h2("Import Data To Databases", align = "center"), br()),
#############################################################

fluidRow(
  useShinyjs(),
  column(5,
         wellPanel(
           selectInput("datatype", "1. Select data type:", 
                       choices = datasets$DataType),
           br()
         )
  ),
  column(5,
         wellPanel(
           uiOutput("file.UI")
         )
  ),
  column(5,
         wellPanel(
           uiOutput("probe.UI")
         )
  )
),
fluidRow(
  column(10,
         wellPanel(
           strong("3. Run the 'Process Data' script:"),
           br(),
           uiOutput("process.UI"),
           br(),
           h4(textOutput("text.process.status"))
         ),
         wellPanel(
           strong("4. Run the 'Import Data' script to upload processed data to DB:"),
           br(),
           uiOutput("import.UI"),
           br(),
           uiOutput("text.import.status")
         ),
         tabsetPanel(
           tabPanel("Processed WQ Data",
                    tableOutput("table.process.wq")
           ),
           tabPanel("Processed Flag Index Data",
                    tableOutput("table.process.flag")
           )
         )
  )
)


#######################################################

) # end UI

########################################################################################
################################    Server   ###########################################
########################################################################################

server <- function(input, output, session) {
  
  
  ### FOLDER/Database SELECTION
  
  # Specify the raw data folder based on data type: (Add paths for other data types as necessary)
  
  ds <- reactive({
    req(input$datatype)
    filter(datasets, DataType == input$datatype)
  })
scriptname <- reactive({ 
    req(input$datatype)
    paste0(getwd(), "/Functions/", ds()$ScriptProcessImport[1])
  })
  
 rawdatafolder <- reactive({
      req(input$datatype)
      ds()$RawFilePath[1]
  })
   processedfolder <- reactive({
        req(input$datatype)
     ds()$ProcessedFilePath[1]
   })  
  filename.db <- reactive({
       req(input$datatype)
    ds()$DBPath[1]
  })
  
  ### FILE SELECTION
  
  # Make the File List
  files <- reactive({grep(
    x = list.files(rawdatafolder(), ignore.case = T, include.dirs = F),
    pattern = "^(?=.*\\bxlsx\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
    value = T,
    perl =T)
  })
  
  # Select Input where user finds and sets the file to import (# this could be set up to do multiple files at a time, but its safer to just do one at a time)
  output$file.UI <- renderUI({
    req(files())
    selectInput(inputId = "file", 
                label = "2. Choose file to upload:", 
                choices = files())
  })
  # Update Select Input when a file is imported (actually when the import button is pressed (succesful or not))
  observeEvent(input$import, {
    updateSelectInput(session = session, 
                      inputId = "file", 
                      label = "2. Choose file to upload:", 
                      choices = files(),
                      selected = input$file)
  })
 # if (input$file == "Profiles") {
  output$probe.UI <- renderUI({
    req(input$datatype == "Profiles")
    selectInput(inputId = "probe", 
                label = "Choose Probe type used for this data:", 
                choices = c("Hydrolab Datasonde3 H20",
                            "YSI_EXO2",
                            "DEP YSI",
                            "Hydrolab MS5"),
                selected = "YSI_EXO2")
  })
 # }                          
                            
  ### Process DATA
  
  # Process Action Button
  output$process.UI <- renderUI({
    req(input$file)
    actionButton(inputId = "process", 
                 label = paste0('Process "', input$file, '" Data'), 
                 width = '500px')
  })
  
  # Run the function to process the data and return 2 dataframes and path as list
  dfs <- eventReactive(input$process,{
    source(scriptname(), local = T) # Hopefully this will overwrite functions as source changes...needs more testing
    PROCESS_DATA(file = input$file, rawdatafolder = rawdatafolder(), filename.db = filename.db(), probe = input$probe())
  })
  
  # Extract each dataframe
  df.wq           <- reactive({dfs()[[1]]})
  path            <- reactive({dfs()[[2]]})
  df.flags        <- reactive({dfs()[[3]]})
  
  # Last File to be Processed
  file.processed <- eventReactive(input$process, {
    input$file
  })
  
  # Text for Process Data Error or Succesful
  process.status <- reactive({
    if(input$file != file.processed()){
      " "
    }else if(inherits(try(df.wq()), "try-error")){
      geterrmessage()
    }else{
      paste0('The file "', input$file, '" was succesfully processed')
    }
  })
  
  # Text Output
  output$text.process.status <- renderText({process.status()})
  
  # Show import button and tables when process button is pressed 
  # Use of req() later will limit these to only show when process did not create an error)
  observeEvent(input$process, {
    show('import')
    show('table.process.wq')
    show('table.process.flag')
  })
  
  ### Import Data
  
  # Import Action Button - Will only be shown when a file is processed succesfully
  output$import.UI <- renderUI({
    req(try(df.wq()))
    actionButton(inputId = "import", 
                 label = paste("Import", file.processed(), "Data"), 
                 width = '500px')
  })
  
  # Import Data - Run import_data function
  observeEvent(input$import, {
    source(scriptname(), local = T)
      IMPORT_DATA(df.wq(), 
                  df.flags(), 
                  path(), 
                  file = input$file, 
                  filename.db = filename.db(), 
                  processedfolder = processedfolder())
  })
  
  # Hide import button and tables when import button is pressed (So one cannot double import same file)
  observeEvent(input$import, {
    hide('import')
    hide('table.process.wq')
    hide('table.process.flag')
  })
  
  # Add text everytime succesful import
  observeEvent(input$import, {
    insertUI(
      selector = "#import",
      where = "afterEnd",
      ui = h4(paste("Succesful import of", nrow(df.wq()), "record(s) in", input$file, "to", input$datatype, "Database"))
    )
  })
  
  ### Table Outputs
  
  # Processed WQ Table - Only make table if processing is succesful
  output$table.process.wq <- renderTable({
    req(try(df.wq()))
    df.wq()
  })
  
  # Processed Flag Table - Only make table if processing is succesful
  output$table.process.flag <- renderTable({
    req(try(df.flags()))
    df.flags()
  })

  
######################################################

# Stop app when browser session window closes
session$onSessionEnded(function() {
      stopApp()
    })

} # end server function
 
#combines the user interface and server (it's a must)
shinyApp(ui = ui, server = server)






