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
  message("Preparing tag sheets for SOMNI db ingestion. Assuming standard Vemco/Innovasea tag sheets.")
  data(cols)
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
  data(dt)
  ct <- dt$metadata_acoustictags
  for (i in 1:length(ct)) {
    if (ct[[i]][1] != class(tags[[i]])[1]) {
      tags[[i]] <- as(tags[[i]], Class = ct[[i]][1])
    }
  }
  return(tags)
}
