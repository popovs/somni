# Connect to db
drv <- RPostgres::Postgres()
host = rstudioapi::askForPassword(prompt = "Host: ")
user = rstudioapi::askForPassword(prompt = "Username: ")
password = rstudioapi::askForPassword(prompt = "Password: ")
db <- DBI::dbConnect(drv = drv,
                     host = host,
                     dbname = "somni",
                     user = user,
                     password = password)
# Get list of all tables in somni schema
tbl <- as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(db, DBI::Id(schema = 'somni'))$table, function(x) slot(x, 'name'))))[[2]]
# Get column names for all tables
cols <- list()
for (t in tbl) {
  cols[[t]] <- names(DBI::dbGetQuery(db, paste("select * from", t, "where false;")))
}
cols <- cols[order(names(cols))] # put in alphabetical order

usethis::use_data(cols, overwrite = TRUE)

dt <- list()
for (t in tbl) {
  dt[[t]] <- lapply(DBI::dbGetQuery(db, paste("select * from", t, "where false;")), class)
}
dt <- dt[order(names(dt))] # put in alphabetical order

usethis::use_data(dt, overwrite = TRUE)

# Get full summary of tables + cols + requirements + data types
db_tbls <- DBI::dbGetQuery(db, "select table_name, column_name, is_nullable, data_type
from information_schema.columns
where table_schema = 'somni' and is_updatable = 'YES'
order by table_name, ordinal_position ;")
usethis::use_data(db_tbls, overwrite = TRUE)
