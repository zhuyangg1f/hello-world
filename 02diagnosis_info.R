# Parsing Registered patient diagnosis information (A04)

a04 <- hl7_df %>% 
  filter(str_trim(hl7triggerevent) == 'A04') %>%
  collect()

fields_ticker04 <- c()
# take a close look
for (i in 1:dim(a04)[1]) {
  # get DG1 row
  # check DG1
  
  check_diagnosis <- function(message) {
    ifelse(grepl('DG1', message),
           "has DG1",
           "DG1 is missing")
  }
  
 
  if(check_diagnosis(a04$hl7msgtext[100]) == "has DG1") {
    headers <- extrac_header(a04$hl7msgtext[100])
    dg1_header <- which(headers== 'DG1')
    
  }
  
  # extract_encounter_num(message) returns the 
  # encounter number associated with the message
  # pass it 1 message, or multiple
  
  # extract_admit_ts(message)
  # return the admission timestamp
  
  # extract_discharge_ts(message)
  # returns discharge timestamp
  
  # extract_message_type(message)
  
  
  
  extrac_header <- function(message) {
    rows <- str_split(message,"\\r")[[1]]
    rows <- rows[rows != ""]
    row_split <- sapply(rows, function(x) strsplit(x, "\\|"))
    headers <- sapply(row_split, '[[', 1)
    attributes(headers) <- NULL
    return(headers)
  }
  
  

  
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  fields_ticker04 <- c(fields_ticker04, length(bb))
}

str_split(a04$hl7msgtext[fields_ticker04==1],"\\r")[[1]][7]
a04$hl7msgtext[fields_ticker04==1]
str_split(a04$hl7msgtext[100],"\\r")[[1]][7]

b <- str_split(a04$hl7msgtext[1],"\\r")[[1]]
xxx <- as.factor(hl7_df$hl7triggerevent)
levels(xxx)

# find patients with DG1 segment

# Extract the first field for each segment (stored as list)
fields01 <- list()
for (i in 1:dim(a04)[1]) {
  element01 <- c()
  for (j in 1:length(str_split(a04$hl7msgtext[i],"\\r")[[1]])){
    element01 <- c(element01, 
                   unlist(str_split(str_split(a04$hl7msgtext[i],"\\r")[[1]][j],"\\|"))[1])
  }
  fields01[[i]] <- element01
}

# Index of all patients with DG1 info
DG1_ticker <- c()
for (i in 1:dim(a04)[1]) {
  if ('DG1' %in% fields01[[i]]) {
    DG1_ticker <- c(DG1_ticker,T)
  } else {
    DG1_ticker <- c(DG1_ticker,F)
  }
}

# Subsetting all these patients
DG1_tbl01 <- a04[DG1_ticker,]

id_list_DG1 <- fields01[DG1_ticker]
fields_ticker_DG1 <- c()
# take a close look
idx <- NA
for (i in 1:dim(DG1_tbl01)[1]) {
  idx <- match('DG1',id_list_DG1[[i]])
  # get DG1 row
  b <- str_split(DG1_tbl01$hl7msgtext[i],"\\r")[[1]][idx]
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  fields_ticker_DG1 <- c(fields_ticker_DG1, length(bb))
}
max(fields_ticker_DG1)
min(fields_ticker_DG1)

DG1_tbl02 <- matrix(nrow=dim(DG1_tbl01)[1], ncol = max(fields_ticker_DG1))
for (i in 1:dim(DG1_tbl01)[1]) {
  idx <- match('DG1',id_list_DG1[[i]])
  # get DG1 row
  b <- str_split(DG1_tbl01$hl7msgtext[i],"\\r")[[1]][idx]
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  DG1_tbl02[i,1:length(bb)] <- bb
}

DG1_tbl02 <- as.data.frame(DG1_tbl02)
colnames(DG1_tbl02) <- c('Segment_type','Set_ID','Diag_Code','Diag_Desc',
                            'Diag_Time','Diag_Type', 'Diag_Category',
                            'Diag_related_group', 'DRG_approval',
                            'DRG_Review_Code','Outlier_Type','Outlier_Days',
                            'Outlier_Cost','Grouper_Version','Diag_Priority')

summary(DG1_tbl02$Diag_Time)  # Most people get PFT (pulmonary function test), 
# LITHOTRIPSY (crush a stone), F/UP LITHO, NELSON, EMS (emergency medical service), METH                    
