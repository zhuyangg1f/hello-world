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



