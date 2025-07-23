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
TRACK_SOURCE_FILE <- TRUE        # Add source tracking to facts

# Comment extraction settings
EXTRACT_COMMENTS <- TRUE                    # Enable comment extraction
COMMENT_PATTERN <- "^([A-Za-z]+):([^:]+)$" # Pattern for business keys
STORE_RAW_COMMENTS <- FALSE                # Store raw comment text
COMMENT_AS_DIMENSION <- TRUE               # Treat business key as dimension

# Output settings
POWERBI_MODE <- TRUE # creates tables with PBI friendly names like FactTable_Main.parquet

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

# Test function for comment extraction
test_comment_extraction <- function() {
  # Create test XML with comment
  test_xml <- '<?xml version="1.0" encoding="UTF-8"?>
<!-- OrderType:B2B -->
<orders>
  <record id="1001" customer="ABC Corp" region="North">
    <order_date>2024-01-15</order_date>
    <items>
      <total_amount>2549.97</total_amount>
      <item_count>3</item_count>
    </items>
  </record>
  <record id="1002" customer="XYZ Ltd" region="South">
    <order_date>2024-01-16</order_date>
    <items>
      <total_amount>1299.99</total_amount>
      <item_count>1</item_count>
    </items>
  </record>
</orders>'
  
  # Write test file
  test_file <- "test_comment_extraction.xml"
  writeLines(test_xml, test_file)
  
  # Test extraction
  cat("Testing comment extraction...\n")
  
  # Parse with enhanced parser
  result <- parse_xml_streaming_with_comments(test_file)
  
  # Check results
  if ("OrderType" %in% names(result)) {
    cat("✓ Successfully extracted business key\n")
    cat(sprintf("  - Business key name: OrderType\n"))
    cat(sprintf("  - Business key value: %s\n", unique(result$OrderType)))
    cat(sprintf("  - Applied to %d records\n", nrow(result)))
  } else {
    cat("✗ Failed to extract business key\n")
  }
  
  # Show sample data
  cat("\nSample data with business key:\n")
  print(head(result[, c("record_id", "customer", "OrderType", "business_key_name", "business_key_value")]))
  
  # Cleanup
  unlink(test_file)
  
  invisible(result)
}

# Alternative patterns for different comment formats
COMMENT_PATTERNS <- list(
  standard = "^([A-Za-z]+):([^:]+)$",           # ABCD:1234
  underscore = "^([A-Za-z_]+):([^:]+)$",        # ABC_DEF:1234
  equals = "^([A-Za-z]+)=([^=]+)$",             # ABCD=1234
  spaced = "^([A-Za-z]+)\\s*:\\s*([^:]+)$",    # ABCD : 1234
  numeric_key = "^([A-Za-z0-9]+):([^:]+)$"     # ABC123:1234
)

# Function to validate comment pattern
validate_comment_pattern <- function(pattern, test_comments) {
  results <- sapply(test_comments, function(comment) {
    grepl(pattern, trimws(comment))
  })
  
  data.frame(
    comment = test_comments,
    matches = results,
    stringsAsFactors = FALSE
  )
}

# Enhanced schema analyzer to handle business keys
update_schema_for_business_keys <- function(schema_info, sample_data) {
  # Check if business key columns exist
  business_key_cols <- c("business_key_name", "business_key_value")
  dynamic_business_keys <- setdiff(
    names(sample_data),
    c(business_key_cols, schema_info$column)
  )
  
  if (length(dynamic_business_keys) > 0 && COMMENT_AS_DIMENSION) {
    # Add dynamic business key columns as dimensions
    new_rows <- data.frame(
      column = dynamic_business_keys,
      classification = "dimension",
      data_type = "string",
      unique_count = sapply(dynamic_business_keys, function(col) {
        n_distinct(sample_data[[col]], na.rm = TRUE)
      }),
      null_ratio = 0,
      numeric_ratio = 0,
      mean_length = sapply(dynamic_business_keys, function(col) {
        mean(nchar(as.character(sample_data[[col]])), na.rm = TRUE)
      }),
      sample_values = sapply(dynamic_business_keys, function(col) {
        paste(head(unique(sample_data[[col]]), 3), collapse = "|")
      }),
      stringsAsFactors = FALSE
    )
    
    schema_info <- rbind(schema_info, new_rows)
    
    log_info(sprintf("Added %d business key dimension(s): %s",
                     length(dynamic_business_keys),
                     paste(dynamic_business_keys, collapse = ", ")))
  }
  
  schema_info
}