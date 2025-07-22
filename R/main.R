# main.R - XML to Parquet Converter Main Entry Point

library(future)
library(furrr)
library(magrittr)
library(data.table)
library(arrow)

# Configuration Constants
XML_FOLDER <- "./input"
OUTPUT_DIR <- "./output"
ID_ATTRIBUTE <- "id"
NUMERIC_THRESHOLD <- 0.8
FACT_PREFIX <- "fact_"
DIM_PREFIX <- "dim_"
ERROR_LOG <- "./logs/error.log"
AUDIT_LOG <- "./logs/audit.log"
BATCH_SIZE <- 50
SCHEMA_SAMPLE_SIZE <- 100

# Schema validation settings
ENABLE_VALIDATION <- TRUE
SCHEMA_DIR <- "./schemas"
VALIDATION_MODE <- "auto"
FAIL_ON_INVALID <- TRUE
SCHEMA_FILE <- NULL

# Audit tracking - source file will be added to every fact record
TRACK_SOURCE_FILE <- TRUE

# Global tracking variables
.processing_start_time <- NULL
.total_files_processed <- 0
.successful_files <- 0
.failed_files <- 0
.validated_files <- 0
.validation_failed <- 0
.validation_skipped <- 0

# Setup parallel processing
setup_parallel <- function() {
  n_cores <- min(parallel::detectCores() - 1, 8)
  plan(multisession, workers = n_cores)
  log_info(sprintf("Initialized %d workers for parallel processing", n_cores))
}

# Main entry point
process_xml_to_parquet <- function(input_folder = XML_FOLDER, output_dir = OUTPUT_DIR) {
  # Initialize tracking
  .processing_start_time <<- Sys.time()
  .total_files_processed <<- 0
  .successful_files <<- 0
  .failed_files <<- 0
  .validated_files <<- 0
  .validation_failed <<- 0
  
  # Setup
  setup_parallel()
  dir.create("logs", showWarnings = FALSE)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Create log header
  log_message(
    sprintf("\n===== NEW PROCESSING SESSION: %s =====", Sys.time()),
    AUDIT_LOG,
    "SESSION"
  )
  
  # Get file list
  xml_files <- list.files(input_folder, pattern = "\\.xml$", full.names = TRUE)
  .total_files_processed <<- length(xml_files)
  
  if (length(xml_files) == 0) {
    log_error("No XML files found in input folder")
    return(invisible())
  }
  
  log_info(sprintf("Found %d XML files in %s", length(xml_files), input_folder))
  
  # Log all files to be processed
  walk(xml_files, ~ log_message(sprintf("Queued: %s", basename(.x)), AUDIT_LOG, "QUEUE"))
  
  # Analyze schema from sample
  log_info("Analyzing schema from sample files...")
  schema_info <- analyze_schema_from_files(head(xml_files, SCHEMA_SAMPLE_SIZE))
  
  # Process in batches
  batches <- split(xml_files, ceiling(seq_along(xml_files) / BATCH_SIZE))
  log_info(sprintf("Processing %d batches of up to %d files each", length(batches), BATCH_SIZE))
  
  # Process batches with tracking
  star_schema_batches <- future_map2(
    batches,
    seq_along(batches),
    ~ process_batch_with_tracking(.x, .y, schema_info),
    .progress = TRUE,
    .options = furrr_options(
      packages = c("xml2", "data.table", "magrittr"),
      globals = c("parse_xml_with_validation", "build_star_schema", 
                  "ENABLE_VALIDATION", "SCHEMA_FILE", "ID_ATTRIBUTE")
    )
  )
  
  # Write results
  log_info("Writing results...")
  write_results_with_summary(star_schema_batches, output_dir)
  
  # Create reports
  create_processing_report()
  if (ENABLE_VALIDATION) create_validation_report(output_dir)
  
  # Cleanup
  plan(sequential)
  
  invisible(TRUE)
}

# Process batch with tracking
process_batch_with_tracking <- function(batch_files, batch_id, schema_info) {
  log_info(sprintf("Starting batch %d with %d files", batch_id, length(batch_files)))
  
  # Process files with validation if enabled
  file_results <- batch_files %>%
    future_map(
      ~ parse_xml_with_validation(.x, validate = ENABLE_VALIDATION, schema_file = SCHEMA_FILE),
      .progress = FALSE
    )
  
  # Count results
  successful <- sum(map_chr(file_results, "status") == "success")
  validation_errors <- sum(map_chr(file_results, "status") == "validation_error") 
  parse_errors <- sum(map_chr(file_results, "status") == "error")
  
  # Update global counters
  .successful_files <<- .successful_files + successful
  .failed_files <<- .failed_files + validation_errors + parse_errors
  .validation_failed <<- .validation_failed + validation_errors
  
  # Log batch summary
  log_batch_summary(batch_id, length(batch_files), successful, validation_errors + parse_errors)
  
  # Extract successful data
  successful_data <- file_results %>%
    keep(~ .x$status == "success") %>%
    map("data") %>%
    rbindlist(fill = TRUE)
  
  # Create error summary
  error_summary <- file_results %>%
    keep(~ .x$status %in% c("error", "validation_error")) %>%
    map_df(~ data.frame(
      file = basename(.x$file),
      error = .x$error,
      type = .x$status,
      stringsAsFactors = FALSE
    ))
  
  # Transform to star schema if we have data
  if (nrow(successful_data) > 0) {
    star_data <- build_star_schema(successful_data, schema_info)
    star_data$error_summary <- error_summary
    return(star_data)
  } else {
    return(list(fact = data.table(), dimensions = list(), error_summary = error_summary))
  }
}