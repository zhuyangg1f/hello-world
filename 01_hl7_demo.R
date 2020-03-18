library(keyring)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)
library(dplyr)
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "PostgreSQL ODBC Driver(UNICODE)",
                      Server   = "172.27.21.215",
                      Database = "msgjournaldb",
                      UID      = 'svcDSAA',
                      PWD      = keyring::key_get("HL7_PASSWORD"),
                      Port     = 5432)


# list tables
db_list_tables(con)

# connect to ms4 tables
ms4 <- tbl(con, "ms4messages")

# count the number of messages 
ms4 %>% 
  count()

ms4 <- ms4 %>%
  filter( id  %in% 268304:269304) %>% 
  collect()

xxx <- ms4 %>% 
  filter(hl7triggerevent == 'A01') %>%
  collect()

text_msg <- xxx$hl7msgtext
print(text_msg[1])
l1 <- length(str_extract_all(text_msg[1],'\\|')[[1]]);l1
l2 <- length(str_extract_all(text_msg[2],'\\|')[[1]]);l2
l3 <- length(str_extract_all(text_msg[3],'\\|')[[1]]);l3
b1 <- str_split(text_msg[1],"\\r")
b7 <- str_split(text_msg[7],"\\r")
b1[[1]][3]
b7[[1]][3]
bb1 <- unlist(str_split(b1[[1]][3],"\\|"))
bb7 <- unlist(str_split(b7[[1]][3],"\\|"))
for (c in 1:length(b[[1]])) {
  
  if (str_starts(b[[1]][c], "PID")) {
    
    bb <- str_split(b[[1]][c], "\\|")
    msgdept <- str_extract(bb[[1]][5], "\\b[A-Z]+")
    msgproc <- str_extract(bb[[1]][5], "\\d+")
    # if this is the correct procedure, save results
    if (dept == msgdept & proc == msgproc) {
      
      # grab message id
      msg_result[msgline,1] <- as.character(x$id[msgline]) 
      # grab report type
      msg_result[msgline,2] <- bb[[1]][26]
      # grab relevant radiologist name
      if (bb[[1]][26] == 'F') {
        
        msg_result[msgline,3] <- str_extract(bb[[1]][33], "[a-zA-Z]+")
      } else {
        
        msg_result[msgline,3] <- str_extract(bb[[1]][34], "[a-zA-Z]+")
      }
    } #if (dept == msgdept & proc == msgproc)
    
  } #if (str_starts(b[[1]][c], "OBR"))
  
} #for (c in 1:length(b[[1]]))
length(str_extract_all(b[[1]][3],'\\|')[[1]])

