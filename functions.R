library(keyring)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)
library(dplyr)
library(lubridate)
library(ggplot2)

# dependency DBI, getPass
connect_postgre <- function(username, password = getPass::getPass(msg = 'password'), server = "172.27.21.215") {
  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = "PostgreSQL ODBC Driver(UNICODE)",
                        Server   = "172.27.21.215",
                        Database = "msgjournaldb",
                        UID      = username,
                        PWD      = password,
                        Port     = 5432)
  return(con)
}

# test code
con <- connect_postgre('yangz')

# denpendency: dplyr
get_ms4_tbl <- function(con) {
  ms4_tbl <- tbl(con, "testmessages") # should change to "ms4messages" for production
  return(ms4_tbl)
}

# test code
hl7_df <- get_ms4_tbl(con)

# dependency: dplyr
get_event <- function(ms4_tbl, event_type) {
  event_tbl <- hl7_df %>% filter(hl7triggerevent == event_type) %>% collect()
  return(event_tbl)
}

# test code
a01 <- get_event(hl7_df, 'A01')

# dependency: dplyr
extract_message <- function(tbl) {
  msg <- tbl %>% select(hl7msgtext) %>% pull()
  return(msg)
}

# test code
msg <- extract_message(a01)

# dependency: none
extract_header <- function(message) {
  rows <- str_split(message,"\\r")[[1]]
  rows <- rows[rows != ""]
  row_split <- sapply(rows, function(x) strsplit(x, "\\|"))
  headers <- sapply(row_split, '[[', 1)
  attributes(headers) <- NULL
  return(headers)
}

# test code
msg_header <- extract_header(msg[1])
xxx <- sapply(msg, extract_header)

#' -----------------------------------------------------------------------------
# Main functions:
# Check whether a message has desired segment
check_segment <- function(message, segment) {
  ifelse(grepl(segment, message),
         "has segment",
         "segment is missing")
}

# Get desired segment row index in text msg
get_segment_index <- function(message, segment) {
  if(check_segment(message, segment) == "has segment") {
    headers <- extract_header(message)
    segment_header <- which(headers == segment)
  } else {
    segment_header <- NA
  }
  return(segment_header)
}

get_segment <- function(message, segment) {
  b <- str_split(message,"\\r")
  segment_out <- b[[1]][get_segment_index(message, segment)]
  return(segment_out)
}

get_all_segments <- function(messages, segment) {
  segments <- unlist(lapply(messages, get_segment, segment = segment))
  return(segments)
}

xx <- get_all_segments(msg,'NK1')

# get_segment_length <- function(message, segment) {
#   # get segment row number
#   row_number <- unlist(lapply(message, get_segment_index, segment = segment))
# }



## Check number of fields of PID
max_seg_length <- function(message,segment) {
  row_number <- unlist(lapply(message, get_segment_index, segment = segment))
  b <- str_split(message,"\\r")
  bb <- sapply(b, "[[", row_number)
  
}

# test code
max_seg_length(msg,'PID')

# get_PID_tbl <- function(message,segment) {
#   # create an empty matrix for intaking number
#   PID_table <- matrix(nrow=length(message), 
#                       ncol = max_seg_length(message,segment))
#   for (i in 1:length(message)) {
#     # get segment row
#     b <- str_split(a01$hl7msgtext[i],"\\r")[[1]][3]
#     # split fields by '|'
#     bb <- unlist(str_split(b,"\\|"))
#     PID_table_01[i,1:length(bb)] <- bb
#   }

# get encounter number
extract_encounter_num <- function(message) {
  b <- str_split(message,"\\r")
  bb <- sapply(b, "[", 3) # need modification here
  bbb <- str_split(bb,"\\|")
  encounter_string <- sapply(bbb, "[", 19)
  encounter_str_split <- str_split(encounter_string, "\\^")
  encounter_num <- sapply(encounter_str_split, "[", 1)
  return(encounter_num)
}

extract_encounter_num(msg)
# testing
extracted_enc <- extract_encounter_num(msg)
real_enc <- a01 %>% select(hl7encounter) %>% pull() %>% str_trim()
sum(extracted_enc != real_enc)

# get msg time
extract_msg_time <- function(message) {
  b <- str_split(message,"\\r")
  bb <- sapply(b, "[", 1) # need modification here
  bbb <- str_split(bb,"\\|")
  msg_time <- lubridate::as_datetime(sapply(bbb, "[", 7)) # diff btw msg time and event time
  return(msg_time)
}

# test on admit time
msg_time <- extract_msg_time(msg)
msg_time_real <- a01 %>% select(hl7msgdatetime) %>% pull()
sum(msg_time_real != msg_time)

# test on discharge time
a03 <- get_event(hl7_df, 'A03')
msg03 <- extract_message(a03)
msg_time <- extract_msg_time(msg03)
msg_time_real <- a03 %>% select(hl7msgdatetime) %>% pull()
sum(msg_time_real != msg_time)

