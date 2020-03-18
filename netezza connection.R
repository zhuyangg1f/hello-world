library(CHART)

con <- DBI::dbConnect(odbc::odbc(), driver = "NetezzaSQL", 
                      database = "UDMH_PROD", uid =  getPass::getPass('username'), 
                      pwd = getPass::getPass('password'), 
                      server = "edwcluster02")
db_list_tables(con)

inp_fact_tbl <- fact_tbl(con, "PATIENT_ADMIN_EVENT_FACT")

