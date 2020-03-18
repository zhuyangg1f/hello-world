#'  ***********************************************************************
#'  ***********************************************************************
#'  ***********************************************************************
#'  Script name: HL7 msgtext data exploring.R
#'  Initially created by: Yang Zhu
#'  Date created: 2020/03/10
#'  Maintainer information: 
#'
#'  Script contents: This contains code for exploring HL7 messages text in
#'                   testmessages table.
#'                   It examines the meaning of all fields in PID part.
#'
#'   
#'  ***********************************************************************

# Set working directory
setwd("C:/Users/ZhuYa/Desktop/COBRA")

# Required packages
library(keyring)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)
library(dplyr)
library(lubridate)
library(ggplot2)

# Database connection
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "PostgreSQL ODBC Driver(UNICODE)",
                      Server   = "172.27.21.215",
                      Database = "msgjournaldb",
                      UID      = 'yangz',
                      PWD      = keyring::key_get("HL7_PASSWORD"),
                      Port     = 5432)

# list tables
db_list_tables(con)

# connect to ms4 tables
test_msg <- tbl(con, "testmessages")

# Collect all data on database (Not recommonded)
hl7_df <- test_msg %>% collect()

# Disconnect from database
DBI::dbDisconnect(con)

#' -----------------------------------------------------------------------------

# Check consistency between triggerevent columns and msgtext

## recording the consistency
trig_ticker <- c() 
for (i in 1:dim(hl7_df)[1]) {
  # get event row
  b <- str_split(hl7_df$hl7msgtext[i],"\\r")[[1]][2]
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  if (str_trim(hl7_df$hl7triggerevent[i]) == bb[2]) {
    trig_ticker <- c(trig_ticker, T)
  } else {
    trig_ticker <- c(trig_ticker, F)
  }
}

### check consistency
sum(trig_ticker) == dim(hl7_df)[1] 

### Not consistent (531 cases matched)
sum(!trig_ticker)

### Subsetting the unmatched cases

unmatch_hl7 <- hl7_df[!trig_ticker,]
unmatch_hl7$hl7triggerevent <- as.factor(unmatch_hl7$hl7triggerevent)
levels(unmatch_hl7$hl7triggerevent) 
### only M02, S12, S14 event has inconsistency

# Parsing PID with admission event (A01)

a01 <- hl7_df %>% 
  filter(str_trim(hl7triggerevent) == 'A01') %>%
  collect()

## Check number of fields of PID
fields_ticker <- c()
for (i in 1:dim(a01)[1]) {
  # get PID row
  b <- str_split(a01$hl7msgtext[i],"\\r")[[1]][3]
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  fields_ticker <- c(fields_ticker, length(bb))
}

## Inconsistent fields number
print(fields_ticker)
max(fields_ticker)
min(fields_ticker)

## Checking the fields_ticker with number 24
check_24 <- a01 %>% filter(fields_ticker == 24)
PID_check <- str_split(check_24$hl7msgtext,"\\r")[[1]][3]
PID_check_fields <- unlist(str_split(PID_check,"\\|"))
print(PID_check_fields) # birth place 'PERU'

## format all fields into a PID table

### Note: ncol should be the maximum fields in PID
PID_table_01 <- matrix(nrow=dim(a01)[1], ncol = max(fields_ticker))

for (i in 1:dim(a01)[1]) {
  # get PID row
  b <- str_split(a01$hl7msgtext[i],"\\r")[[1]][3]
  # split fields by '|'
  bb <- unlist(str_split(b,"\\|"))
  PID_table_01[i,1:length(bb)] <- bb
}

## transfer PID table into data frame
PID_table_01 <- as.data.frame(PID_table_01)

## Add presumptive columns names and leave unknow ones
colnames(PID_table_01) <- c('Segment_type','Set_ID','External_ID',
                            'Internal_ID','Alternate_ID','Patient_Name',
                            'Mother_Maiden_Name','DOB','Sex','Alias',
                            'Race','Address','County_Code','Home_Phone',
                            'Business_Phone','Language','Marital',
                            'Religion','Account_Number','SSN_number',
                            'Driver_License','Mother_Identifier','Ethnic',
                            'Birth_place')

## Subset of dataset for checking correctness of col names (send to Wendy Gushev)
PID_table_check <- head(PID_table_01,10)
PID_table_check <- PID_table_check[,1:20]
write.csv(PID_table_check, file = 'PID_table_presum_colnames')
write.table(PID_table_check, "PID_table_presum_colnames.txt", sep="\t")

#' -----------------------------------------------------------------------------

# Data Cleaning and Manipulation

## remove redundant variables (all NA or only few obs)
PID_tbl <- PID_table_01 %>%
           select(-c(Segment_type, Set_ID, Mother_Maiden_Name, Alias, Race,
                     SSN_number, Driver_License, Mother_Identifier, Ethnic, 
                     Birth_place))

## Replace empty string with NA
PID_tbl <- PID_tbl %>% mutate_all(na_if,"")

### make a copy of dataset
PID_tbl_copy = PID_tbl

## Modify the columns contain ID

### External ID
PID_tbl$External_ID <- as.character(PID_tbl$External_ID)
for (i in 1:dim(PID_tbl)[1]) {
    PID_tbl$External_ID[i] <- unlist(str_split(PID_tbl$External_ID[i],'\\^'))[1]
}
PID_tbl$External_ID <- as.factor(PID_tbl$External_ID)

#### Check if External ID have the same length
External_length <- unlist(lapply(PID_tbl$External_ID, str_length))
sum(External_length  == External_length [1]) # check mark

### Internal ID
PID_tbl$Internal_ID <- as.character(PID_tbl$Internal_ID)
for (i in 1:dim(PID_tbl)[1]) {
  PID_tbl$Internal_ID[i] <- unlist(str_split(PID_tbl$Internal_ID[i],'\\^'))[1]
}
PID_tbl$Internal_ID <- as.factor(PID_tbl$Internal_ID)

#### Check if Internal ID have the same length
Internal_length <- unlist(lapply(PID_tbl$Internal_ID, str_length))
sum(Internal_length  == Internal_length [1]) # check mark

### Alternate ID
PID_tbl$Alternate_ID <- as.character(PID_tbl$Alternate_ID)
for (i in 1:dim(PID_tbl)[1]) {
  PID_tbl$Alternate_ID[i] <- unlist(str_split(PID_tbl$Alternate_ID[i],'\\^'))[1]
}
PID_tbl$Alternate_ID <- as.factor(PID_tbl$Alternate_ID)

#### Check if account numbers have the same length
Alternate_length <- unlist(lapply(PID_tbl$Alternate_ID, str_length))
sum(Alternate_length  == Alternate_length [1], na.rm = T) # unmatched length

### Parsing Patient Name
PID_tbl$Patient_Name <- as.character(PID_tbl$Patient_Name)
for (i in 1:dim(PID_tbl)[1]) {
  PID_tbl$Family_Name[i] <- unlist(str_split(PID_tbl$Patient_Name[i],'\\^'))[1]
  PID_tbl$Given_Name[i] <- unlist(str_split(PID_tbl$Patient_Name[i],'\\^'))[2]
  PID_tbl$Middle_Name[i] <- unlist(str_split(PID_tbl$Patient_Name[i],'\\^'))[3]
  PID_tbl$Suffix[i] <- unlist(str_split(PID_tbl$Patient_Name[i],'\\^'))[4]
  PID_tbl$Prefix[i] <- unlist(str_split(PID_tbl$Patient_Name[i],'\\^'))[5]
}
PID_tbl$Prefix <- as.factor(PID_tbl$Prefix)

### Convert DOB to date
PID_tbl$DOB <- as.Date(PID_tbl$DOB, "%Y%m%d")

### Account Number (Encounter ID)
PID_tbl$Account_Number <- as.character(PID_tbl$Account_Number)
for (i in 1:dim(PID_tbl)[1]) {
  PID_tbl$Account_Number[i] <- unlist(str_split(PID_tbl$Account_Number[i],'\\^'))[1]
}
PID_tbl$Account_Number <- as.factor(PID_tbl$Account_Number)

#### Check if account numbers have the same length
account_length <- unlist(lapply(PID_tbl$Account_Number, str_length))
sum(account_length  == account_length [1]) # check mark

#### Check consistency between encounter ID and Account Number (Consistent)
any((str_trim(a01$hl7encounter) == as.character(PID_tbl$Account_Number)) == F)

### Add created time by encounter number
admit_time = a01$hl7msgdatetime
PID_tbl <- cbind(PID_tbl, admit_time)

### Check duplication
sum(duplicated(PID_tbl$Account_Number)) 
PID_tbl[duplicated(PID_tbl$Account_Number),] # due to different msg date/time

#### Subset data by unique encounter ID
PID_tbl <- PID_tbl[!duplicated(PID_tbl$Account_Number),]

#' -----------------------------------------------------------------------------

# Parsing discharge event (A03)

## Subsetting A03 event
a03 <- hl7_df %>% 
  filter(str_trim(hl7triggerevent) == 'A03') %>%
  collect()

## Link Discharge and admission event by encounter ID

### Subsetting a03 by the patients admitted in a01 dataset
discharged_a03 <- a03[str_trim(a03$hl7encounter) %in% PID_tbl$Account_Number,]

### Get patient discharge date/time
discharge_time <- discharged_a03$hl7msgdatetime

### Get corresponding patient encounter
discharge_encounter <- str_trim(discharged_a03$hl7encounter)

### Combine encounter ID and discharge time as a new dataframe
discharge_addon <- cbind.data.frame(discharge_encounter,discharge_time)
colnames(discharge_addon) <- c('Account_Number','discharge_time')

## Merge PID table with discharge time table

### Keep same levels before merging
combined <- sort(union(levels(PID_tbl$Account_Number), 
                       levels(discharge_addon$Account_Number)))

sum(duplicated(discharge_addon$Account_Number)) # 11 duplications

### Delete duplication
discharge_addon <- discharge_addon[!duplicated(discharge_addon$Account_Number),]

## PID tbl with both admit and discharge time 
AD_tbl <- inner_join(mutate(PID_tbl, Account_Number=factor(Account_Number, 
                                                           levels=combined)),
               mutate(discharge_addon, Account_Number=factor(Account_Number, 
                                                             levels=combined)))
#' -----------------------------------------------------------------------------

# AD_tbl Analysis

## create variable: duration of stay (LOS)

AD_tbl$LOS <- difftime(AD_tbl$discharge_time, AD_tbl$admit_time, units = "days")
AD_tbl$LOS <- as.numeric(AD_tbl$LOS)

###histogram of LOS
hist(AD_tbl$LOS, breaks = 20)

###density plot of LOS
plot(density(AD_tbl$LOS))

## create variable: age of patients
age <- year(AD_tbl$discharge_time)- year(AD_tbl$DOB) #Change to msg date
AD_tbl$Age <- as.numeric(age)

###histogram of age
hist(age)

## create the time of the day patient has been admitted (daytime, nighttime)
hour(AD_tbl$admit_time)
hist(hour(AD_tbl$admit_time))
AD_tbl <- AD_tbl %>% mutate(day_night = ifelse( hour(admit_time) %in% 8:20,
                  yes = 'Day', no = 'Night'))
AD_tbl$day_night <- as.factor(AD_tbl$day_night)

## EDA (LOS)

### Histogram of LOS (with density)
AD_tbl %>% ggplot(aes(LOS)) + 
           geom_histogram(aes(y=..density..)) +
           geom_density(col = "red") +
           labs(title='Distribution of LOS')


## Preliminary Gamma GLM

res.glm.Gamma.log <- glm(formula = LOS ~ Sex + Marital + Age + day_night,
                         family  = Gamma(link = "inverse"),
                         data    = AD_tbl)

summary(res.glm.Gamma.log)

X_train <- AD_tbl %>% select(-LOS)
y_pred <- predict(res.glm.Gamma.log, newdata=X_trian, type="response")

hist(AD_tbl$LOS, breaks = 20,prob=T,ylim=c(0,0.5))
# predicted LOS density
lines(density(y_pred))

rmse <- sqrt(sum((y_pred-AD_tbl$LOS)^2/518)); rmse
sd(AD_tbl$LOS) # performance very bad
