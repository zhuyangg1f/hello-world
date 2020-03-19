#' Extract Encounter Number
#'
#' Extract patient's encounter number from a list of HL7 messages text
#' @param messages a list of string, each element represents a HL7 message text
#' @keywords extract
#' @return a vector of string, each string is a encounter number 
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' extracted_enc <- extract_encounter_num(msg)
#' extracted_enc
#' }
extract_encounter_num <- function(messages) {
  b <- get_all_segments(messages, 'PID')
  bb <- sapply(b, str_split, pattern = "\\|")
  attributes(bb) <- NULL
  encounter_string <- sapply(bb, "[", 19)
  encounter_str_split <- str_split(encounter_string, "\\^")
  encounter_num <- sapply(encounter_str_split, "[", 1)
  return(encounter_num)
}


#' Extract HL7 message time
#'
#' Extract HL7 message time from 'MSH' segment. It can be used to get patient admit time or discharge time
#' @param messages a list of string, each element represents a HL7 message text
#' @keywords extract
#' @return a vector of string, each string is the HL7 message date & time 
#' @export
#' @examples
#' \dontrun{
#' msg01 <- extract_message(a01)
#' admit_time <- extract_msg_time(msg01)
#' msg03 <- extract_message(a03)
#' discharge_time <- extract_msg_time(msg03)
#' }
extract_msg_time <- function(messages) {
  b <- get_all_segments(messages, 'MSH')
  bb <- sapply(b, str_split, pattern = "\\|")
  attributes(bb) <- NULL
  msg_time <- lubridate::as_datetime(sapply(bb, "[", 7))
  return(msg_time)
}
