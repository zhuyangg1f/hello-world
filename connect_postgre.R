#' Create a connection to the SMH Postgre
#'
#' Connect to the St Michael's Hospital PostgreSQL ODBC driver.
#' @param username Username
#' @param password Password to connect to Postgre. Default will display a pop-up window to type password
#' @keywords connect
#' @return coonect_postgre() returns an S4 object that inherits from DBI::DBIconnection. This object is used to communicate with the Postgre tables
#' @export
#' @examples
#' \dontrun{
#' con <- connect_postgre(username = 'myusername')
#' hl7_df <- ms4_tbl("ms4messages")
#' head(hl7_df)
#' }
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


#' Connect to a Postgre MS4 message table
#'
#' Connect to a MS4 message table from the PostgreSQL ODBC driver
#' @param con A connection to the edw obtained from `connect_postgre()`
#' @keywords table
#' @return a `tibble` containing HL7 trigger event, patient encounter ID, message date and time, Raw 
#' HL7 message, patient name, and etc
#' @export
#' @examples
#' \dontrun{
#' con <- connect_postgre(username = 'myusername')
#' hl7_df <- ms4_tbl("ms4messages")
#' head(hl7_df)
#' }
ms4_tbl <- function(con) {
  tbl <- dplyr::tbl(con, "ms4messages")
  return(tbl)
}


#' Get Event table
#'
#' Subsetting desired Event table from MS4 table
#' @param tbl A MS4 message table obtained from `ms4_tbl`
#' @param event_type A desired event type, such as 'A01' (Admission event), 'A03' (Discharge event)
#' @keywords table
#' @return a `tibble` containing HL7 trigger event (only the desired trigger event), patient encounter ID, 
#' message date and time, Raw HL7 message, patient name, and etc
#' @export
#' @examples
#' \dontrun{
#' con <- connect_postgre(username = 'myusername')
#' hl7_df <- ms4_tbl("ms4messages")
#' a01 <- get_event(hl7_df, 'A01')
#' head(a01)
#' }
get_event_tbl <- function(tbl, event_type) {
  event_tbl <- tbl %>% filter(hl7triggerevent == event_type) %>% collect()
  return(event_tbl)
}
