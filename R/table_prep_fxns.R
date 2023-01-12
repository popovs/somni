#' Prepare Vemco/Innovasea tag sheets for SOMNI db
#'
#' @description This function takes Vemco/Innovasea sales order tag sheets,
#' performs simple data validation, selects necessary columns for SOMNI db,
#' and ensures all columns are coerced to the correct data type.
#'
#' @param tags Dataframe or tibble of tag sheet data. Multiple tag sheets can be merged into one dataframe.
#'
#' @return A dataframe with 45 columns, matching the table structure in the SOMNI db table `metadata_acoustictags`.
#' @export
#'
#' @examples
#' \dontrun{
#' TagSheet <- read.csv("TagSheet.csv")
#' prep_tag_sheet(TagSheet)
#' }
prep_tag_sheet <- function(tags) {
  # Data checks
  message("Preparing tag sheets for SOMNI db ingestion. Assuming standard Vemco/Innovasea sales order tag sheets.")
  stopifnot("Supplied tag sheet must be class 'data.frame' or 'tibble'." = any(class(tags) %in% c("data.frame", "tibble")))
  if (length(tags) < 44) stop("There should be at least 44 columns from 'Sales Order' to 'Ship Date', inclusive. Some columns are missing from your data.")
  if (length(tags) > 44) warning("Your tag sheet has greater than 44 columns. SOMNI db currently does not support HTI and ADST columns, and will therefore skip importing those columns.")
  # Begin processing
  tags <- janitor::clean_names(tags)
  tags <- dplyr::select(tags, sales_order:ship_date)
  # Coerce date coltypes
  tags$ship_date <- as.Date(tags$ship_date)
  tags$date_updated <- NA
  tags$date_updated <- as.POSIXct(tags$date_updated)
  names(tags) <- cols$metadata_acoustictags
  # Make sure col classes match db table data types
  #data(dt)
  ct <- dt$metadata_acoustictags
  for (i in 1:length(ct)) {
    if (ct[[i]][1] != class(tags[[i]])[1]) {
      tags[[i]] <- as(tags[[i]], Class = ct[[i]][1])
    }
  }
  return(tags)
}

#' Prepare OTN tagging metadata sheet for SOMNI db
#'
#' @param dat A dataframe containing OTN tagging metadata, including the template header with the OTN logo, template version number, and instructions.
#' @param db Name of SOMNI database connection object in R workspace. Defaults to "db".
#'
#' @return A list containing two dataframes, each corresponding to the respective table in SOMNI db: `metadata_animals` and `acoustic_animals`.
#' @export
#'
#' @examples
#' \dontrun{
#' drv <- RPostgres::Postgres()
#' db <- DBI::dbConnect(drv = drv,
#'                      host = host,
#'                      dbname = "somni",
#'                      user = user,
#'                      password = password)
#'
#' tm <- readxl::read_excel("OTN_tagging_sheet.xslx", sheet = "Tag Metadata")
#' prep_otn_tagging(tm, db = db)
#' }
prep_otn_tagging <- function(dat, db = db) {
  # First check what data type it is. Some  people might
  # import full excel file w both tabs into R; others might
  # import the single tab into a single df. On error just
  # say to supply a single df since that's easier.
  if ("data.frame" %in% class(dat)) {
    ft <- dat
  } else if (class(dat) == "list") {
    message("Assuming 'Tag Metadata' tab in the OTN data template contains the data.")
    ft <- dat[[grep("Tag|tag", names(dat))]]
  } else {
    stop("Supplied must be a dataframe of the standard OTN tagging data Excel template, including the first few rows containing the data sheet version number and OTN logo.")
  }

  orig_n <- nrow(ft)
  ft[ft %in% c("NA", "")] <- NA

  # Prepare input data ----
  # Pull out the first full row - the first row w complete cases
  # is the header. Then clean up.
  ft_names <- ft[complete.cases(ft),]
  if (nrow(ft_names) > 1) ft_names <- ft_names[1,] # Select first row in case it also pulls out "Sample Data" row
  ft_names <- as.character(ft_names)
  ft_names <- janitor::make_clean_names(ft_names)
  ft_names[1] <- "animal_id"

  # Anything BEFORE the first complete row is the metadata for the OTN sheet
  meta <- ft[1:(grep(TRUE, complete.cases(ft))-1),]
  # Extract template version number
  v <- meta[[which(grepl("Version|version", meta))]]
  v <- v[grep("Version|version", v)]
  v <- readr::parse_number(v)

  # Extract data
  ft <- ft[-(1:grep(TRUE, complete.cases(ft))),]
  # If "Sample Data" row still present, find and remove it
  if (any(grepl("Sample Data", ft))) {
    s_yn <- TRUE
    # Assuming "Sample Data" present in column 1
    ft <- ft[-grep("Sample Data", ft[[1]]),]
  }

  # Set header names
  names(ft) <- ft_names

  # Check if sheet has sat tags
  has_sat <- any(grepl("satellite|satelite|sat", tolower(ft$tag_type)))

  # Connect to db and query ----
  # At this point, need to connect to the db to fill in the tables.
  # Check if db is connected. If not, connect now.
  if (missing(db)) {
    # TO-DO: tryCatch series in case someone entered credentials incorrectly.
    drv <- RPostgres::Postgres()
    rstudioapi::showDialog("Connecting to database...", message = "You will now be prompted for your SOMNI db host, username, and password, so have those handy. This is used to run data validation and ensure the data you're processing right now will be compatible with the database.")
    host = rstudioapi::showPrompt(title = "Host: ", message = "Host: ", default = "e.g., 123.225.99.456")
    user = rstudioapi::showPrompt(title = "DB user", message = "DB user (default 'forager' for read-only access): ", default = "forager")
    password = rstudioapi::askForPassword(prompt = "Password: ")
    db <- DBI::dbConnect(drv = drv,
                         host = host,
                         dbname = "somni",
                         user = user,
                         password = password)
  }

  # Pull relevant data from SOMNI db for building tables, e.g. primary keys.
  max_somni_id <- DBI::dbGetQuery(db, "select max(somni_id) from metadata_animals;")[[1]]
    if (is.na(max_somni_id)) max_somni_id <- 99999
  max_animal_tag <- DBI::dbGetQuery(db, "select max(animal_tag) from acoustic_animals;")[[1]]
    if (is.na(max_animal_tag)) max_animal_tag <- 0

  if (has_sat) {
    max_sat_sa <- DBI::dbGetQuery(db, "select max(animal_tag) from satellite_animals;")[[1]]
      if (is.na(max_sat_sa)) max_sat_sa <- 0
    max_sat_ms <- DBI::dbGetQuery(db, "select max(sattag_pk) from metadata_sattags;")[[1]]
      if (is.na(max_sat_ms)) max_sat_ms <- 0
  }

  # Species taxon IDs
  spp <- unique(ft$scientific_name)
  spp <- stringr::str_to_sentence(spp)
  spp_id <- toString(shQuote(spp))
  sql <- paste("select taxon_key, scientific_name from species where scientific_name in (", spp_id, ");")
  spp_id <- DBI::dbGetQuery(db, sql)

  if (nrow(spp_id) != length(spp)) {
    not_in <- spp[!(spp %in% spp_id$scientific_name)]
    not_in <- toString(shQuote(not_in))
    warning("The following species in your data cannot be matched to a species in the\nSOMNI db species table:\n\n",
            not_in,
            "\n\nEnsure there are no typos in the species name and that they match standard\nnomenclature used in Fishbase/SeaLifeBase.")
  }

  ft <- merge(ft, spp_id)

  # SOMNI IDs
  # Needs to be done this way since sometimes 1 animal has multiple tags
  ids <- as.data.frame(unique(ft$animal_id))
  names(ids) <- "animal_id"
  ids$somni_id <- max_somni_id + as.numeric(rownames(ids))
  ft <- merge(ft, ids)

  # Create empty dfs with SOMNI fields to populate with OTN data
  #data(cols)
  ma <- data.frame(matrix(ncol = length(cols$metadata_animals), nrow = nrow(ft)))
  names(ma) <- cols$metadata_animals

  if (has_sat) {
    aa <- data.frame(matrix(ncol = length(cols$acoustic_animals), nrow = length(grep("acoustic|vemco", tolower(ft$tag_type)))))
    names(aa) <- cols$acoustic_animals
    sa <- data.frame(matrix(ncol = length(cols$satellite_animals), nrow = length(grep("sat", tolower(ft$tag_type)))))
    names(sa) <- cols$satellite_animals
    ms <- data.frame(matrix(ncol = length(cols$metadata_sattags), nrow = nrow(sa)))
    names(ms) <- cols$metadata_sattags
  } else {
    aa <- data.frame(matrix(ncol = length(cols$acoustic_animals), nrow = nrow(ft)))
    names(aa) <- cols$acoustic_animals
  }

  # Clean up dates ----
  ft[,ft_names[grep("date", ft_names)]] <-
    as.data.frame(
      lapply(ft[,ft_names[grep("date", ft_names)]],
                         function(x) {
                           if (class(x) == "character") {
                             janitor::convert_to_datetime(x)
                             } else {
                             janitor::excel_numeric_to_date(x)
                             }
                         }))

  # Populate ma and aa ----
  # As column names may change across version numbers, this is
  # where to check for which version we're on to translate OTN
  # columns to SOMNI db columns.
  # This function was developed with template v4.2 as default.
  message("Detected OTN tagging metadata template version ", v)

  if (v < 4.2) ft$harvest_date <- ft$tag_activation_date # harvest_date col does not exist in v <4.2

  # Clean/prep some fields ----
  # Length
  ft$length_type <- toupper(ft$length_type)
  ft$length2_type <- toupper(ft$length2_type)
  ft$length_m <- round(as.numeric(ft$length_m), 3)
  ft$length2_m <- round(as.numeric(ft$length2_m), 3)
  ft$weight_kg <- round(as.numeric(ft$weight_kg), 3)
  # Sex
  ft$sex <- toupper(ft$sex)
  sex_uncertain <- ifelse(is.na(ft[["comments"]][which(grepl("\\?", ft$sex))]),
                          "Sex uncertain.",
                          paste0(ft[["comments"]][which(grepl("\\?", ft$sex))], "; Sex uncertain."))
  ft[["comments"]][which(grepl("\\?", ft$sex))] <- sex_uncertain
  ft$sex <- gsub("\\?", "", ft$sex)
  # Wild/hatchery
  ft$wild_or_hatchery <- toupper(ft$wild_or_hatchery)

  # Populate metadata_animals ma ----
  # First lets populate metadata_animals.
  ma$somni_id <- ft$somni_id
  ma$fieldbook_id <- ft$animal_id
  ma$species_id <- ft$taxon_key
  ma$recaptured <- FALSE
  ma$capture_datetime <- ft$harvest_date
  ma$capture_timezone <- 'UTC'
  ma$capture_location <- ft$capture_location
  ma$capture_latitude <- suppressWarnings(parzer::parse_lat(ft$capture_latitude))
  ma$capture_longitude <- suppressWarnings(parzer::parse_lon(ft$capture_longitude))
  ma$capture_depth <- as.numeric(ft$capture_depth_m)
  ma$depth_unit <- 'm'
  ma$dead_sample <- FALSE
  ma$water_temperature <- as.numeric(ft$holding_temperature_degrees_c)
  ma$water_do <- as.numeric(ft$dissolved_oxygen_ppm)
  ma$length_unit <- 'm'
  ma$weight_unit <- 'kg'
  if (length(ft[["length_m"]][!is.na(ft$length_type == "STANDARD")]) > 0) ma[["standard_length"]][ft$length_type == "STANDARD"] <- ft[["length_m"]][ft$length_type == "STANDARD"]
  if (length(ft[["length_m"]][!is.na(ft$length_type == "TOTAL")]) > 0) ma[["total_length"]][ft$length_type == "TOTAL"] <- ft[["length_m"]][ft$length_type == "TOTAL"]
  if (length(ft[["length_m"]][!is.na(ft$length_type == "FORK")]) > 0) ma[["fork_length"]][ft$length_type == "FORK"] <- ft[["length_m"]][ft$length_type == "FORK"]
  if (length(ft[["length_m"]][!is.na(ft$length_type %in% c("DISC", "DISC WIDTH", "WIDTH", "WINGSPAN"))]) > 0) ma[["disc_width"]][ft$length_type %in% c("DISC", "DISC WIDTH", "WIDTH", "WINGSPAN")] <- ft[["length_m"]][ft$length_type %in% c("DISC", "DISC WIDTH", "WINGSPAN")]
  if (length(ft[["length2_m"]][!is.na(ft$length2_type == "STANDARD")]) > 0) ma[["standard_length"]][ft$length2_type == "STANDARD"] <- ft[["length2_m"]][ft$length2_type == "STANDARD"]
  if (length(ft[["length2_m"]][!is.na(ft$length2_type == "TOTAL")]) > 0) ma[["total_length"]][ft$length2_type == "TOTAL"] <- ft[["length2_m"]][ft$length2_type == "TOTAL"]
  if (length(ft[["length2_m"]][!is.na(ft$length2_type == "FORK")]) > 0) ma[["fork_length"]][ft$length2_type == "FORK"] <- ft[["length2_m"]][ft$length2_type == "FORK"]
  if (length(ft[["length2_m"]][!is.na(ft$length2_type %in% c("DISC", "DISC WIDTH", "WIDTH", "WINGSPAN"))]) > 0) ma[["disc_width"]][ft$length2_type %in% c("DISC", "DISC WIDTH", "WIDTH", "WINGSPAN")] <- ft[["length2_m"]][ft$length2_type %in% c("DISC", "DISC WIDTH", "WINGSPAN")]
  ma$weight <- ft$weight_kg
  ma$sex <- ft$sex
  ma$age <- ft$age
  ma$age_unit <- ft$age_units
  ma$wild_hatchery <- ifelse(ft$wild_or_hatchery %in% c("W", "WILD"),
                             0, 1)
  ma$stock <- ft$stock
  ma$finclips_dna <- ft$dna_sample_taken
  ma$release_datetime <- ft$utc_release_date_time
  ma$release_timezone <- 'UTC'
  ma$release_location <- ft$release_location
  ma$release_group <- ft$release_group
  ma$release_latitude <- suppressWarnings(parzer::parse_lat(ft$release_latitude))
  ma$release_longitude <- suppressWarnings(parzer::parse_lon(ft$release_longitude))
  ma$notes <- ft$comments
  ma$date_updated <- as.Date(ma$date_updated)

  # Remove any duplicated rows (caused if one animal
  # has multiple tags)
  ma <- ma[!duplicated(ma), ]

  # Split ft into acoustic & sat ----
  # Split up ft into ft acoustic and ft satellite
  # Now that ma is complete, split up ft row-wise by tag type
  if (has_sat) {
    ft_s <- ft[grep("sat", tolower(ft$tag_type)),]
    ft <- ft[grep("acoustic", tolower(ft$tag_type)),]
    }

  # Populate acoustic_animals aa ----
  # Next populate acoustic_animals.
  # Functionally, 'ft' now only has acoustic data.
  # If sat tag data is present, it is now in 'ft_s' (see above code)
  aa$animal_tag <- max_animal_tag + as.numeric(rownames(aa))
  aa$somni_id <- ft$somni_id
  aa$vue_id <- ft$tag_id_code
  aa$tag_serial <- ft$tag_serial_number
  aa$vue_tag_id <- paste0(stringr::word(ft$tag_code_space, sep = "-", start = 1, end = 2), "-", aa$vue_id)
  aa$vue_tag_id <- ifelse(toupper(ft$tag_type) == "ACOUSTIC", aa$vue_tag_id, NA)
  aa$activation_datetime <- ft$tag_activation_date
  aa$activation_timezone <- 'UTC'
  aa$implant_type <- ft$tag_implant_type
  aa$implant_method <- ft$tag_implant_method
  aa$tagger <- ft$tagger
  aa$tagging_pi <- ft$tag_owner_pi
  aa$tagging_organization <- ft$tag_owner_organization
  aa$holding_time <- ft$preop_hold_period
  aa$recovery_time <- ft$postop_hold_period
  aa$holding_temperature <- ft$holding_temperature_degrees_c
  aa$holding_do <- ft$dissolved_oxygen_ppm
  aa$surgery_datetime <- as.Date(ft$date_of_surgery)
  aa$surgery_timezone <- 'UTC'
  aa$surgery_location <- ft$surgery_location
  aa$surgery_latitude <- suppressWarnings(parzer::parse_lat(ft$surgery_latitude))
  aa$surgery_longitude <- suppressWarnings(parzer::parse_lon(ft$surgery_longitude))
  aa$sedative <- ft$sedative
  aa$sedative_ppm <- ft$sedative_concentration_ppm
  aa$anesthetic <- ft$anaesthetic
  aa$buffer <- ft$buffer
  aa$anesthetic_ppm <- ft$anaesthetic_concentration_ppm
  aa$buffer_anesthesia_ppm <- ft$buffer_concentration_in_anaesthetic_ppm
  aa$anesthetic_recirc_ppm <- ft$anaesthetic_concentration_in_recirculation_ppm
  aa$buffer_recirc_ppm <- ft$buffer_concentration_in_recirculation_ppm
  aa$notes <- ifelse(toupper(ft$tag_type) == "ACOUSTIC", NA, "Satellite tag")
  aa$date_updated <- as.Date(aa$date_updated)
  aa$recaptured <- FALSE

  # Populate satellite tag data ----
  if (has_sat) {
    # Populate tag metadata first
    ms$sattag_pk <- max_sat_ms + as.numeric(rownames(ms))
    ms$program_number <- ft_s$tag_id_code
    ms$sattag_sn <- ft_s$tag_serial_number
    ms$sattag_model <- ft_s$tag_model
    ms$sattag_brand <- ft_s$tag_manufacturer
    ms$sattag_owner <- apply(cbind(ft_s$tag_owner_organization, ft_s$tag_owner_pi), 1,
                             function(x) paste(x[!is.na(x)], collapse = ", "))
    ms[["sattag_owner"]][ms$sattag_owner == ""] <- NA
    ms$tag_life <- ft_s$est_tag_life
    ms$activation_datetime <- ft_s$tag_activation_date
    ms$activation_timezone <- 'UTC'

    # Next populate satellite_animals sa
    sa$animal_tag <- max_sat_sa + as.numeric(rownames(sa))
    sa$somni_id <- ft_s$somni_id
    sa$sattag_pk <- ms$sattag_pk
    sa$attachment_method <- ft_s$tag_implant_type
    sa$attachment_datetime <- ft_s$date_of_surgery
    sa$attachment_timezone <- 'UTC'
    sa$initialization_datetime <- ft_s$tag_activation_date
    sa$initialization_timezone <- 'UTC'
  }

  # Return message ----
  # Number of rows of original data, minus metadata rows,
  # minus header row, minus sample data row (if present)
  nrow_dat <- ifelse(s_yn, (orig_n - nrow(meta) - 2), (orig_n - nrow(meta) - 1))
  nrow_final <- ifelse(has_sat, nrow(ft) + nrow(ft_s), nrow(ft))
  message(nrow_final, " records out of the original ", nrow_dat, " records were successfully prepared for SOMNI db import. This includes:
          * ", nrow(ma), " unique animal records (metadata_animals)
          * ", nrow(aa), " acoustically tagged animals (acoustic_animals)
          * ", ifelse(has_sat, nrow(sa), 0), " satellite tagged animals (satellite_animals + metadata_sattags).
          As always, check that all your data looks correct after processing.

          ASSUMPTIONS:
          * Assuming no animals in this sheet were recaptured.
          * Assuming all timestamps in UTC.
          * Assuming all lengths in m.
          * Assuming all weights in kg.
          * Assuming values present in the ", ifelse(v < 4.2, "TAG_ACTIVATION_DATE", "HARVEST_DATE"), " column are equivalent to capture date/time.
          * FLOY tag columns left blank; FLOY tags will need to be manually added as the OTN sheet does not have a dedicated FLOY column.
          * ", length(sex_uncertain), " records had a ? in the sex column, therefore the phrase 'sex uncertain' was added to the comments column but the ? was removed. E.g., 'F?' becomes 'F', but with a note added to the comments column that the sex was uncertain.")

  # TO-DO: write function that coerces column types and call it here.

  out <- list()
  out[["metadata_animals"]] <- ma
  out[["acoustic_animals"]] <- aa
  if (has_sat) {
    out[["metadata_sattags"]] <- ms
    out[["satellite_animals"]] <- sa
  }
  return(out)

}



#' Prepare OTN instrument metadata sheet for SOMNI db
#'
#' @param dat A dataframe containing OTN deployment metadata, including the template header with the OTN logo, template version number, and instructions.
#' @param db Name of SOMNI database connection object in R workspace. Defaults to "db".
#'
#' @return A list containing up to 11 dataframes, either corresponding to their respective table in SOMNI db or containing errors that need to be fixed.
#' @export
#'
#' @examples
#' \dontrun{
#' drv <- RPostgres::Postgres()
#' db <- DBI::dbConnect(drv = drv,
#'                      host = host,
#'                      dbname = "somni",
#'                      user = user,
#'                      password = password)
#'
#' dm <- readxl::read_excel("OTN_deployments.xslx", sheet = "Deployment")
#' prep_otn_deployment(dm, db = db)
#' }
prep_otn_deployment <- function(dat, db = db) {
  # First check what data type it is. Some  people might
  # import full excel file w both tabs into R; others might
  # import the single tab into a single df. On error just
  # say to supply a single df since that's easier.
  if ("data.frame" %in% class(dat)) {
    d <- dat
  } else if (class(dat) == "list") {
    message("Assuming 'Deployment' tab in the OTN data template contains the data.")
    d <- dat[[grep("Deploy|deploy", names(dat))]]
  } else {
    stop("Supplied must be a dataframe of the standard OTN deployment data Excel template, including the first few rows containing the data sheet version number and OTN logo.")
  }

  orig_n <- nrow(d)
  d[d %in% c("NA", "")] <- NA

  # Prepare input data ----
  # Pull out the first full row - the first row w complete cases
  # is the header. Then clean up.
  d_names <- d[complete.cases(d),]
  if (nrow(d_names) > 1) d_names <- d_names[1,] # Select first row in case it also pulls out "Sample Data" row
  d_names <- as.character(d_names)
  d_names <- janitor::make_clean_names(d_names)

  # Anything BEFORE the first complete row is the metadata for the OTN sheet
  meta <- d[1:(grep(TRUE, complete.cases(d))-1),]
  # Extract template version number (if it has it)
  has_v <- length(which(grepl("Version|version", meta))) > 0
  if (has_v) {
    v <- meta[[which(grepl("Version|version", meta))]]
    v <- v[grep("Version|version", v)]
    v <- readr::parse_number(v)
  }

  # Extract data
  d <- d[-(1:grep(TRUE, complete.cases(d))),]
  s_yn <- FALSE
  # If "Sample Data" row still present, find and remove it
  if (any(grepl("Sample Data", d))) {
    s_yn <- TRUE
    # Assuming "Sample Data" present in column 1
    d <- d[-grep("Sample Data", d[[1]]),]
  }

  # Set header names
  names(d) <- d_names

  # Connect to db and query ----
  # At this point, need to connect to the db to fill in the tables.
  # Check if db is connected. If not, connect now.
  if (missing(db)) {
    # TO-DO: tryCatch series in case someone entered credentials incorrectly.
    drv <- RPostgres::Postgres()
    rstudioapi::showDialog("Connecting to database...", message = "You will now be prompted for your SOMNI db host, username, and password, so have those handy. This is used to run data validation and ensure the data you're processing right now will be compatible with the database.")
    host = rstudioapi::showPrompt(title = "Host: ", message = "Host: ", default = "e.g., 123.225.99.456")
    user = rstudioapi::showPrompt(title = "DB user", message = "DB user (default 'forager' for read-only access): ", default = "forager")
    password = rstudioapi::askForPassword(prompt = "Password: ")
    db <- DBI::dbConnect(drv = drv,
                         host = host,
                         dbname = "somni",
                         user = user,
                         password = password)
  }

  # Pull relevant data from SOMNI db for building tables, e.g. primary keys.
  max_deploy_id <- DBI::dbGetQuery(db, "select max(deploy_id) from deployments_retrievals;")[[1]]
  if (is.na(max_deploy_id)) max_deploy_id <- 0

  stations_list <- DBI::dbGetQuery(db, "select station_id, \"array\", station_name, waterbody_name from metadata_stations;")
  max_station_id <- max(stations_list$station_id)
  if (is.na(max_station_id)) max_station_tag <- 99999

  receivers_list <- DBI::dbGetQuery(db, "select receiver_sn from metadata_receivers;")
  releases_list <- DBI::dbGetQuery(db, "select release_sn from metadata_releases;")

  sensors_list <- DBI::dbGetQuery(db, "select sensor_id, sensor_sn from metadata_sensors;")
  max_sensor_id <- max(sensors_list$sensor_id)
  if (is.na(max_sensor_id)) max_sensor_id <- 99999

  # List of all currently deployed gear in the deployments_retrievals table
  db_deployed <- DBI::dbGetQuery(db, "select * from deployment_metadata where retrieval_status = 0;")
  #db_deployed$station_gear <- paste0(db_deployed$station, db_deployed$release_sn, db_deployed$receiver_sn)
  #db_deployed$rel_rec <- paste0(db_deployed$release_sn, db_deployed$receiver_sn)

  # Match stations ----
  if (nrow(d[is.na(d$station_no),]) > 0) warning("Some records in your deployment file are missing a station name. Those records will be ignored when processing the data. All deployments must be associated with a station name.")
  d <- d[!is.na(d$station_no),]

  # Merge station names
  # Ideally merge on OTN array-station name combo
  # 1) Split into two dfs: rows with array provided,
  #    (d_array) and rows without array provided (d_sta)
  # 2) Merge on array-station combo or just station
  #    for the respective dfs
  # 3) Combine the results back into one df ('d2')
  # 4) If nrow(d) != nrow(d2), then there are
  #    multiple stations with the same name, and that
  #    needs to be resolved by the user.
  # 5) Then for any stations that are not matched to
  #    a station name, prompt user if they wish to make
  #    new stations.
  # Below fails.. returns same n of rows even if multiple matches
  # ifelse(!is.na(d$otn_array) & !is.na(d$station_no),
  #        as.numeric(merge(d, stations_list, by.x = c("otn_array", "station_no"), by.y = c("array", "station_name"), all.x = T)[["station_id"]]),
  #        as.numeric(merge(d, stations_list, by.x = c("station_no"), by.y = c("station_name"), all.x = T)[["station_id"]])
  #        )
  # Split array vs. non-array rows, then merge
  d_array <- d[!is.na(d$otn_array),]
  d_sta <- d[is.na(d$otn_array),]

  if (nrow(d_array) > 0) d_array <- merge(d_array, stations_list, by.x = c("otn_array", "station_no"), by.y = c("array", "station_name"), all.x = T)
  if (nrow(d_sta) > 0) d_sta <- merge(d_sta, stations_list, by.x = c("station_no"), by.y = c("station_name"), all.x = T)

  d2 <- rbind(d_array, d_sta)
  rm(d_array)
  rm(d_sta)

  # Now compare rowcounts and deal with duplicate matches
  while (nrow(d) != nrow(d2)) {
    message("Some of your station names appear multiple times in the database. Would you like to interactively pick the correct station in the console?\n1 - Yes, let's choose the correct stations now\n2 - No, I'll modify the original dataset to include the array name and/or fix any typos I find and re-run prep_otn_deployment().")
    choice <- readline(prompt = "Option (1 or 2): ")
    while(!(choice %in% c(1,2))) {
      cat("Sorry, didn't catch your previous response as a 1 or 2.")
      choice <- readline(prompt = "Option (1 or 2): ")
    }
    if (choice == 2) {
      stop("\rOk, exiting function now.")
    } else {
      # Pull out duplicated station rows
      bad_sta <- plyr::count(d2$station_no)[plyr::count(d2$station_no)[2] > 1,]
      message("You will now be prompted to choose the correct station for several stations. Just enter the number from the 'station_choice' column for the correct one.")
      for (i in 1:nrow(bad_sta)) {
        j <- stations_list[stations_list$station_name == bad_sta$x[i],]
        row.names(j) <- NULL
        j$station_choice <- row.names(j)
        print(j[c("station_choice", "station_name", "array", "waterbody_name")])
        corr <- readline(prompt = "Correct station: ")
        while(!(corr %in% j$station_choice)) {
          cat("Sorry, that was not a valid option. Choose the correct station and enter the corresponding number from the 'station_choice' column. \n\n")
          print(j[c("station_choice", "station_name", "array", "waterbody_name")])
          corr <- readline(prompt = "Correct station: ")
        }
        if (!exists("sta_fixes")) {
          sta_fixes <- j[j$station_choice == corr, c("station_name", "station_id")]
        } else {
          sta_fixes <- rbind(sta_fixes, j[j$station_choice == corr, c("station_name", "station_id")])
        }

      }

      # Now remove any rows from d2 where
      # station_name IS IN but (and)
      # station_id IS NOT IN sta_fixes!
      d2 <- d2[!((d2$station_no %in% sta_fixes$station_name) & !(d2$station_id %in% sta_fixes$station_id)),]

      # Since this is wrapped in 'while' loop, it will
      # run through these fixes until the rowcount lines up.
    }

  } # end while(nrow(d2) != nrow(d))

  d <- d2
  rm(d2)

  # New stations will be processed in the section "New stations" below

  # Retrievals + Lost ----
  # First, need to set previous years' deployments to 'retrieved'
  # Since OTN doesn't use deploy_ids, need to match up to SOMNI
  # deploy_id using a station-gear key (jk, not doing this - because Hussey lab fills out OTN form chaotically)
  # First filter out any 'recovered' or 'lost' data
  # TODO: This will fail if OTN updates the column names w/o adding version numbers to templates...
  lost0 <- d[grep('l', d$recovered_y_n_l, ignore.case = T), ]
  retrievals0 <- d[!is.na(d$recover_date_time_yyyy_mm_dd_thh_mm_ss),] #d[grep('y', d$recovered_y_n_l, ignore.case = T), ]

  if (nrow(lost0) > 0) {
    lost0$station_id <- as.integer(lost0$station_id)
    lost <- as.data.frame(unique(db_deployed[db_deployed$station_id %in% lost0$station_id, "deploy_id"]))
    names(lost) <- "deploy_id"
    lost$retrieval_status <- 2
  }

  if (nrow(retrievals0) > 0) {
    retrievals0$station_id <- as.integer(retrievals0$station_id)
    retrieve_success_id <- as.data.frame(unique(db_deployed[db_deployed$station_id %in% retrievals0$station_id, c("deploy_id", "station_id")]))
    retrievals0 <- merge(retrievals0, retrieve_success_id, by = "station_id")

    # Make retrievals output df
    retrievals <- as.data.frame(retrievals0$deploy_id)
    names(retrievals) <- "deploy_id"
    retrievals$retrieval_status <- 1
    retrievals$retrieval_datetime <- janitor::convert_to_datetime(retrievals0$recover_date_time_yyyy_mm_dd_thh_mm_ss)
    retrievals$retrieval_timezone <- "UTC"
    retrievals$retrieval_latitude <- suppressWarnings(parzer::parse_lat(retrievals0$recover_lat_dd_ddddd))
    retrievals$retrieval_longitude <- suppressWarnings(parzer::parse_lon(retrievals0$recover_long_ddd_ddddd))
    retrievals$retrieved_by <- retrievals0$deployed_by_lead_technicians

    # I.e., this is the last known location of these receivers - need retrieval info!
    # TODO: unretrieved_sensors
    unretrieved <- db_deployed[db_deployed$deploy_id %!in% retrievals$deploy_id,]
    unretrieved_receiver <- unretrieved[unretrieved$receiver_sn %in% d$ins_serial_no & !is.na(unretrieved$receiver_sn),]
    unretrieved_release <- unretrieved[unretrieved$release_sn %in% d$ar_serial_no & !is.na(unretrieved$release_sn),]

  }

  if ((nrow(lost0) + nrow(retrievals0)) > 2) {
    retrievals <- dplyr::bind_rows(lost, retrievals)
  }

  # New stations ----
  # Extract new stations
  ns.x <- d[is.na(d$station_id),]
  if (any(tolower(ns.x$recovered_y_n_l) != "new")) message("These deployments aren't marked as 'new', but the stations don't match any existing stations in the database. Therefore assuming the following ", nrow(ns.x), " stations are new (in addition to any already marked as 'new'): ", paste(ns.x$station_no, collapse = " "))

  # Create empty df to populate with new station metadata
  if (nrow(ns.x) > 0) {
    ns <- setNames(data.frame(matrix(nrow = nrow(ns.x), ncol = length(cols$metadata_stations))), cols$metadata_stations)
    ns$station_id <- max_station_id + as.numeric(row.names(ns))
    ns$station_name <- ns.x$station_no
    ns$station_retired <- FALSE
    ns$date_established <- janitor::excel_numeric_to_date(as.numeric(ns.x$deploy_date_time_yyyy_mm_dd_thh_mm_ss))
  }

  rm(ns.x)

  # New gear ----
  # Create df with new releases
  # VR2ARs are combo receiver-release, and are saved to the "metadata_receivers"
  # table rather than to "metadata_releases"
  n_rel.x <- d[d$ar_serial_no %!in% releases_list$release_sn & !is.na(d$ar_serial_no),]
  n_rel.x <- n_rel.x[!grepl("VR2AR", n_rel.x$ar_model_no),]
  if(nrow(n_rel.x) > 0) {
    n_rel <- setNames(data.frame(matrix(nrow = nrow(n_rel.x), ncol = length(cols$metadata_releases))), cols$metadata_releases)
    n_rel$release_sn <- n_rel.x$ar_serial_no
    # TODO: make release brand/model smarter... at the moment
    # I'm just taking the first word as "brand" and anything after that
    # as "model"
    n_rel$release_brand <- stringr::str_split(n_rel.x$ar_model_no, " ", 2, simplify = TRUE)[,1]
    n_rel$release_model <- stringr::str_split(n_rel.x$ar_model_no, " ", 2, simplify = TRUE)[,2]
    n_rel$release_health <- TRUE
    n_rel[n_rel == ""] <- NA
  }

  rm(n_rel.x)

  # Create df with new receivers
  n_rec.x <- d[d$ins_serial_no %!in% receivers_list$receiver_sn & !is.na(d$ins_serial_no),]
  if(nrow(n_rec.x) > 0) {
    n_rec <- setNames(data.frame(matrix(nrow = nrow(n_rec.x), ncol = length(cols$metadata_receivers))), cols$metadata_receivers)
    n_rec$receiver_sn <- n_rec.x$ins_serial_no
    n_rec$receiver_model <- n_rec.x$ins_model_no
    n_rec$receiver_health <- TRUE
    n_rec$code_set <- n_rec.x$code_set
    n_rec[n_rec == ""] <- NA
  }

  rm(n_rec.x)

  # Create df with new sensors
  # TODO: Update if OTN changes their ins column names
  # TODO: Unsure what to do with 'transmitter' - probably
  #       'deploy' it as a tag? To do later.
  # TODO: make brand/model smarter - same issue as 'n_rel' above
  n_sen.x <- setNames(d[d$oceanographic_equipment_serial %!in% sensors_list$sensor_sn & !is.na(d$oceanographic_equipment_serial), c("oceanographic_equipment_model", "oceanographic_equipment_serial")], c("model", "sn"))
  n_sen.x <- rbind(n_sen.x, setNames(d[d$marine_mammal_hydrophone_serial %!in% sensors_list$sensor_sn & !is.na(d$marine_mammal_hydrophone_serial), c("marine_mammal_hydrophone_model", "marine_mammal_hydrophone_serial")], c("model", "sn")))
  if(nrow(n_sen.x) > 0) {
    n_sen <- setNames(data.frame(matrix(nrow = nrow(n_sen.x), ncol = length(cols$metadata_sensors))), cols$metadata_sensors)
    n_sen$sensor_id <- max_sensor_id + as.numeric(row.names(n_sen))
    n_sen$sensor_sn <- n_sen.x$sn
    n_sen$sensor_brand <- stringr::str_split(n_sen.x$model, " ", 2, simplify = TRUE)[,1]
    n_sen$sensor_model <- stringr::str_split(n_sen.x$model, " ", 2, simplify = TRUE)[,2]
    n_sen$sensor_health <- TRUE
    n_sen[n_sen == ""] <- NA
  }

  rm(n_sen.x)

  # Deployments ----
  # Finally, deploy the current year's gear

  # Choose records where deployment actually happened
  dd <- d[!is.na(d$deploy_date_time_yyyy_mm_dd_thh_mm_ss), ]
  ddrow <- nrow(dd)

  # Merge any new stations to dd
  if (exists("ns")) {
    dd <- merge(dd, ns[,c("station_name", "station_id")], by.x = "station_no", by.y = "station_name", all.x = TRUE)
    dd$station_id <- ifelse(is.na(dd$station_id.x),
                            as.numeric(dd$station_id.y), # R plays funky with Integer64 datatype, so converting to numeric
                            as.numeric(dd$station_id.x))
  }

  if(any(is.na(dd$station_id))) warning("Some station_ids are NA.")
  if(ddrow != nrow(dd)) warning("Merging new station_ids into dd resulted in extra records.")

  # Merge any sensor IDs to dd
  # First combine sensors list w new sensors
  if (exists("n_sen")) sensors_list <- rbind(sensors_list, n_sen[,c("sensor_id", "sensor_sn")])
  # Now merge
  dd <- merge(dd, sensors_list,
              by.x = "oceanographic_equipment_serial",
              by.y = "sensor_sn",
              all.x = TRUE)
  dd <- merge(dd, sensors_list,
              by.x = "marine_mammal_hydrophone_serial",
              by.y = "sensor_sn",
              all.x = TRUE)

  if(ddrow != nrow(dd)) warning("Merging new sensor_ids into dd resulted in extra records.")

  # Create deploy_id
  # Needs to be done this way because sometimes one station will
  # get tons of gear and have multiple rows per OTN sheet
  deploy_ids <- as.data.frame(unique(dd$station_no))
  names(deploy_ids) <- "station_no"
  deploy_ids$deploy_id <- max_deploy_id + as.numeric(row.names(deploy_ids))
  dd <- merge(dd, deploy_ids, all.x = TRUE)
  if(ddrow != nrow(dd)) warning("Merging new deploy_ids into dd resulted in extra records.")

  # Create deployments df
  dr <- setNames(data.frame(matrix(nrow = nrow(dd), ncol = length(cols$deployments_retrievals))), cols$deployments_retrievals)
  dr$deploy_id <- dd$deploy_id
  dr$station_id <- dd$station_id
  dr$deploy_datetime <- janitor::excel_numeric_to_date(as.numeric(dd$deploy_date_time_yyyy_mm_dd_thh_mm_ss), include_time = TRUE, tz = "UTC") # Assuming UTC
  dr$deploy_timezone <- "UTC"
  dr$deploy_latitude <- parzer::parse_lat(dd$deploy_lat)
  dr$deploy_longitude <- parzer::parse_lon(dd$deploy_long)
  dr$deploy_depth <- as.numeric(dd$bottom_depth)
  dr$depth_unit <- "m"
  dr$riser_length <- as.numeric(dd$riser_length)
  dr$length_unit <- "m"
  dr$instrument_depth <- dr$deploy_depth - dr$riser_length
  dr$deployed_by <- dd$deployed_by_lead_technicians
  dr$deploy_notes <- dd$comments
  dr$retrieval_status <- 0
  dr <- unique(dr) # TODO: Might not work if there's some deploy_ids where user just didn't enter e.g. depth for one record but did for another

  if(any((plyr::count(dr$deploy_id)[,2]) > 1)) warning("Duplicate deploy_ids generated when extracting deployments-retrievals.")

  # Deploy gear ----

  # deployment_release
  d_rel <- setNames(data.frame(matrix(nrow = nrow(dd), ncol = length(cols$deployment_release))), cols$deployment_release)
  d_rel$deploy_id <- dd$deploy_id
  d_rel$release_sn <- dd$ar_serial_no
  d_rel$temp <- dd$ar_model_no
  d_rel <- d_rel[!grepl("VR2AR", d_rel$temp),] # We exclude AR2ARs from release metadata
  d_rel <- d_rel[,1:(length(d_rel) - 1)] # Get rid of temp column
  d_rel <- d_rel[!is.na(d_rel$release_sn),]
  d_rel <- unique(d_rel) # In case deployments have same release but multiple receivers

  # deployment_receiver
  d_rec <- setNames(data.frame(matrix(nrow = nrow(dd), ncol = length(cols$deployment_receiver))), cols$deployment_receiver)
  d_rec$deploy_id <- dd$deploy_id
  d_rec$receiver_sn <- dd$ins_serial_no
  d_rec <- d_rec[!is.na(d_rec$receiver_sn),]
  d_rec <- unique(d_rec)

  # deployment_sensor
  d_sen.x <- setNames(dd[,c("deploy_id", "sensor_id.x")], c("deploy_id", "sensor_id"))
  d_sen.x <- rbind(d_sen.x, setNames(dd[,c("deploy_id", "sensor_id.y")], c("deploy_id", "sensor_id")))
  d_sen.x <- d_sen.x[!is.na(d_sen.x$sensor_id),]
  d_sen <- setNames(data.frame(matrix(nrow = nrow(d_sen.x), ncol = length(cols$deployment_sensor))), cols$deployment_sensor)
  d_sen$deploy_id <- d_sen.x$deploy_id
  d_sen$sensor_id <- d_sen.x$sensor_id
  d_sen <- unique(d_sen)
  rm(d_sen.x)

  # Return dfs ----
  # The following dfs are returned, where applicable:
  # 1) Retrievals (retrieval success) TODO: add support for retrieval lat/long
  # 2) New stations
  # 3) New releases
  # 4) New receivers
  # 5) New sensors
  # 6) Deployment metadata
  # 7) Deployed releases
  # 8) Deployed receivers
  # 9) Deployed sensors
  # 10) Errors: unretrieved receivers, releases, and (TODO) sensors

  out <- list()
  out$retrievals <- retrievals
  if (exists("ns")) out$metadata_stations <- ns
  if (exists("n_rel")) out$metadata_releases <- n_rel
  if (exists("n_rec")) out$metadata_receivers <- n_rec
  if (exists("n_sen")) out$metadata_sensors <- n_sen
  out$deployments_retrievals <- dr
  out$deployment_release <- d_rel
  out$deployment_receiver <- d_rec
  out$deployment_sensor <- d_sen
  out$error_unretrieved_release <- unretrieved_release
  out$error_unretrieved_receiver <- unretrieved_receiver

  # Return message ----
  # Number of rows of original data, minus metadata rows,
  # minus header row, minus sample data row (if present)
  nrow_dat <- ifelse(s_yn, (orig_n - nrow(meta) - 2), (orig_n - nrow(meta) - 1))
  message(nrow(retrievals), " recoveries and ", nrow(dr), " new deployments out of ", nrow_dat, " records were successfully prepared for SOMNI db import. This includes:
          * ", ifelse(exists("lost0"), nrow(lost0), 0), " lost stations
          * ", ifelse(exists("retrievals0"), nrow(retrievals0), 0), " successfully retrieved stations
          * ", ifelse(exists("ns"), nrow(ns), 0), " newly created stations (metadata_stations)

    New pieces of gear detected includes:
          * ", ifelse(exists("n_rel"), nrow(n_rel), 0), " new releases (metadata_releases)
          * ", ifelse(exists("n_rec"), nrow(n_rec), 0), " new receivers (metadata_receivers)
          * ", ifelse(exists("n_sen"), nrow(n_sen), 0), " new oceanographic + hydrophone sensors (metadata_sensors)

    Total deployed gear includes:
          * ", nrow(d_rel), " deployed releases (deployment_release; note VR2ARs are included in deployment_receiver)
          * ", nrow(d_rec), " deployed receivers (deployment_receiver)
          * ", nrow(d_sen), " deployed sensors (deployment_sensor)

    Finally, some errors were detected. Certain pieces of gear are currently
    still marked as 'deployed' in the database, with no retrieval information.
    This includes ", nrow(unretrieved_release), " releases and ", nrow(unretrieved_receiver), " receivers.

          ASSUMPTIONS:
          * Assuming all timestamps in UTC.
          * Assuming all depths/lengths in m.")

  return(out)

}
