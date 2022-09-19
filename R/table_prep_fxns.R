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
#' TagSheet <- read.csv("TagSheet.csv")
#' prep_tag_sheet(TagSheet)
prep_tag_sheet <- function(tags) {
  message("Preparing tag sheets for SOMNI db ingestion. Assuming standard Vemco/Innovasea sales order tag sheets.")
  #data(cols)
  stopifnot("Supplied tag sheet must be class 'data.frame' or 'tibble'." = (class(tags) %in% c("data.frame", "tibble")))
  tags <- janitor::clean_names(tags)
  tags <- dplyr::select(tags, sales_order:ship_date)
  stopifnot("There should be exactly 44 columns from 'Sales Order' to 'Ship Date', inclusive. \nSOMNI db currently does not support HTI columns." = (length(tags) == 44))
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

#' Prepare OTN tagging metadata sheets for SOMNI db
#'
#' @param dat A dataframe containing OTN tagging metadata, including the template header with the OTN logo, template version number, and instructions.
#' @param db Name of database connection object in R workspace. Defaults to "db".
#'
#' @return A list containing two dataframes, each corresponding to the respective table in SOMNI db: `metadata_animals` and `acoustic_animals`.
#' @export
#'
#' @examples
#' drv <- RPostgres::Postgres()
#' db <- DBI::dbConnect(drv = drv,
#'                      host = host,
#'                      dbname = "somni",
#'                      user = user,
#'                      password = password)
#'
#' tm <- readxl::read_excel("OTN_tagging_sheet.xslx", sheet = "Tag Metadata")
#' prep_otn_tagging(tm, db = db)
prep_otn_tagging <- function(dat, db = db) {
  # First check what data type it is. Some  people might
  # import full excel file w both tabs into R; others might
  # import the single tab into a single df. On error just
  # say to supply a single df since that's easier.
  if (class(dat) == "data.frame") {
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
  max_animal_tag <- DBI::dbGetQuery(db, "select max(animal_tag) from acoustic_animals;")[[1]]

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
  aa <- data.frame(matrix(ncol = length(cols$acoustic_animals), nrow = nrow(ft)))
  names(aa) <- cols$acoustic_animals
  # TO-DO: write function that coerces column types and call it here.

  # Populate ma and aa ----
  # As column names may change across version numbers, this is
  # where to check for which version we're on to translate OTN
  # columns to SOMNI db columns.
  if (v == 4.2) { # might make more sense to simply change all colnames to v4.2 colnames
    message("Detected OTN tagging metadata template version 4.2.")

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
    ma$capture_datetime <- janitor::excel_numeric_to_date(as.numeric(ft$harvest_date)) # TO-DO: make this line more robust if dates correctly imported?
    ma$capture_timezone <- 'UTC'
    ma$capture_location <- ft$capture_location
    ma$capture_latitude <- as.numeric(ft$capture_latitude)
    ma$capture_longitude <- as.numeric(ft$capture_longitude)
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
    ma$release_datetime <- janitor::excel_numeric_to_date(as.numeric(ft$utc_release_date_time), include_time = T, tz = "UTC") # TO-DO: make this line more robust if dates correctly imported?
    ma$release_timezone <- 'UTC'
    ma$release_location <- ft$release_location
    ma$release_group <- ft$release_group
    ma$release_latitude <- as.numeric(ft$release_latitude)
    ma$release_longitude <- as.numeric(ft$release_longitude)
    ma$notes <- ft$comments
    ma$date_updated <- as.Date(ma$date_updated)

    # Populate acoustic_animals aa ----
    # Next populate acoustic_animals.
    aa$animal_tag <- max_animal_tag + as.numeric(rownames(aa))
    aa$somni_id <- ma$somni_id
    aa$vue_id <- ft$tag_id_code
    aa$tag_serial <- ft$tag_serial_number
    aa$vue_tag_id <- paste0(stringr::word(ft$tag_code_space, sep = "-", start = 1, end = 2), "-", aa$vue_id)
    aa$vue_tag_id <- ifelse(toupper(ft$tag_type) == "ACOUSTIC", aa$vue_tag_id, NA)
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
    aa$surgery_latitude <- as.numeric(ft$surgery_latitude)
    aa$surgery_longitude <- as.numeric(ft$surgery_longitude)
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

  }

  # Number of rows of original data, minus metadata rows, minus header row, minus sample data row (if present)
  nrow_dat <- ifelse(s_yn, (orig_n - nrow(meta) - 2), (orig_n - nrow(meta) - 1))
  message(nrow(ft), " records out of the original ", nrow_dat, " records were successfully prepared for SOMNI db import.
          ASSUMPTIONS:
          * Assuming no animals in this sheet were recaptured.
          * Assuming all timestamps in UTC.
          * Assuming all lengths in m.
          * Assuming all weights in kg.
          * Assuming values present in the 'HARVEST_DATE' column are equivalent to capture date/time.
          * FLOY tag columns left blank; FLOY tags will need to be manually added as the OTN sheet does not have a dedicated FLOY column.
          * ", length(sex_uncertain), " records had a ? in the sex column, therefore the phrase 'sex uncertain' was added to the comments column but the ? was removed. E.g., 'F?' becomes 'F', but with a note added to the comments column that the sex was uncertain.")

  out <- list()
  out[["metadata_animals"]] <- ma
  out[["acoustic_animals"]] <- aa
  return(out)

}
