#' Extract HL7 message
#'
#' Extract all HL7 message text from ms4message database
#' @param tbl A MS4 message table 
#' @keywords extract
#' @return a vector of string, each string contains one HL7 message text
#' @export
#' @examples
#' \dontrun{
#' hl7_df <- ms4_tbl("ms4messages")
#' a01 <- get_event(hl7_df, 'A01')
#' msg <- extract_message(a01)
#' msg[1]
#' }
extract_message <- function(tbl) {
  msg <- tbl %>% select(hl7msgtext) %>% pull()
  return(msg)
}

#' Extract message segment header (Utility function)
#'
#' Extract all message segment headers from a single HL7 message text
#' @param msg a character string of HL7 message text
#' @keywords extract
#' @return a vector of string, each string contains one segment header. For example, 'MSH','EVN','PID','PV1','DG1' and so on
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' seg_header <- extract_header(msg[1])
#' seg_header
#' }
extract_seg_header <- function(msg) {
  rows <- str_split(msg,"\\r")[[1]]
  rows <- rows[rows != ""]
  row_split <- sapply(rows, function(x) strsplit(x, "\\|"))
  headers <- sapply(row_split, '[[', 1)
  attributes(headers) <- NULL
  return(headers)
}


#' Check segment header (Utility function)
#'
#' Check whether a desired segment header exists in a single HL7 message text
#' @param msg a character string of HL7 message text
#' @param segment a character string as desired segment header, such as 'MSH', 'PID'
#' @return a binary outcome in string, a HL7 message can either "has segment" or "segment is missing"
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' check_segment(msg[1],'PID')
#' check_segment(msg[1],'DG1')
#' }
check_segment <- function(msg, segment) {
  ifelse(grepl(segment, msg),
         "has segment",
         "segment is missing")
}


#' Get segment Index (Utility function)
#'
#' Find which row the desired segment is located if the segment exists. This function can be used to extract 
#' desired segment information from a vector of HL7 message text
#' @param msg a character string of HL7 message text
#' @param segment a character string as desired segment header, such as 'MSH', 'PID'
#' @return an integer which represents the index location of desired segment in a HL7 message text. 
#' Return NA if segment does not exist
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' get_segment_index(msg[1],'MSH')
#' get_segment_index(msg[1],'DG1')
#' }
get_segment_index <- function(msg, segment) {
  if(check_segment(msg, segment) == "has segment") {
    headers <- extract_header(msg)
    segment_header <- which(headers == segment)
  } else {
    segment_header <- NA
  }
  return(segment_header)
}

#' Extract message segment (Utility function)
#'
#' Extract desired message segment from a single HL7 message text
#' @param msg a character string of HL7 message text
#' @param segment a character string as desired segment header, such as 'MSH', 'PID'
#' @keywords extract
#' @return a character string of message segment, or NA if segment does not exist
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' extract_segment(msg[1],'PID')
#' extract_segment(msg[1],'DG1')
#' }
extract_segment <- function(msg, segment) {
  b <- str_split(msg,"\\r")[[1]]
  if (check_segment(msg, segment) == "has segment") {
    segment_out <- b[get_segment_index(msg, segment)]
  } else {
    segment_out = NA
  }
  return(segment_out)
}


#' Extract all desired message segments
#'
#' Extract desired message segment from a vector of HL7 messages text. This function is not applicable to the segment which
#' can appear twice or more times in the message text, such as 'DG1'
#' @param messages a vector of string, each element represents a HL7 message text
#' @param segment a character string as desired segment header, such as 'MSH', 'PID'
#' @keywords extract
#' @return a vector of string, each string is the message content for desired segment. For example 'MSH|XX||20200314||'
#' It may have NA for some elements since corresponding messages text does not have desired segment
#' @export
#' @examples
#' \dontrun{
#' msg <- extract_message(a01)
#' get_all_segments(msg,'MSH')
#' get_all_segments(msg,'PID')
#' }
extract_all_segments <- function(messages, segment) {
  segments <- unlist(lapply(messages, extract_segment, segment = segment)) # we can modify here to accomodatte 'DG1' segment
  return(segments)
}


