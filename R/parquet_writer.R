# parquet_writer.R - Parquet Writing Functions

# Write results with summary
write_results_with_summary <- function(star_batches, output_dir) {
  # Write star schema data
  write_merged_star_schema(star_batches, output_dir)
  
  # Create error summary
  all_errors <- star_batches %>%
    map("error_summary") %>%
    rbindlist(fill = TRUE)
  
  if (nrow(all_errors) > 0) {
    # Write error summary
    error_file <- file.path(output_dir, "processing_errors.csv")
    write.csv(all_errors, error_file, row.names = FALSE)
    log_info(sprintf("Error summary written to %s", error_file))
  }
  
  # Create processing manifest
  create_processing_manifest(output_dir)
  
  # Document schema
  if (exists("schema_info") && nrow(schema_info) > 0) {
    document_schema(schema_info, file.path(output_dir, "schema_documentation.csv"))
  }
}

# Write merged star schema
write_merged_star_schema <- function(star_batches, output_dir) {
  # Remove NULL batches
  star_batches <- compact(star_batches)
  
  if (length(star_batches) == 0) {
    log_error("No data to write")
    return(invisible())
  }
  
  # Create output directory
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Merge and write fact table
  write_fact_table(star_batches, output_dir)
  
  # Merge and write dimensions
  write_dimension_tables(star_batches, output_dir)
  
  # Create metadata file
  create_parquet_metadata(output_dir)
}

# Write fact table
write_fact_table <- function(star_batches, output_dir) {
  log_info("Writing fact table...")
  
  # Extract all fact tables
  fact_tables <- star_batches %>%
    map("fact") %>%
    keep(~ nrow(.x) > 0)
  
  if (length(fact_tables) == 0) {
    log_warning("No fact data to write")
    return()
  }
  
  # Merge all fact tables
  merged_facts <- rbindlist(fact_tables, fill = TRUE)
  
  # Write to parquet
  fact_file <- file.path(output_dir, paste0(FACT_PREFIX, "main.parquet"))
  arrow::write_parquet(
    merged_facts,
    fact_file,
    compression = "snappy",
    use_dictionary = TRUE
  )
  
  log_audit(sprintf("Wrote fact table: %d rows, %.2f MB", 
                    nrow(merged_facts), 
                    file.size(fact_file) / 1024^2))
}

# Write dimension tables
write_dimension_tables <- function(star_batches, output_dir) {
  log_info("Writing dimension tables...")
  
  # Get all dimension names
  all_dimensions <- star_batches %>%
    map("dimensions") %>%
    keep(~ length(.x) > 0)
  
  if (length(all_dimensions) == 0) {
    log_warning("No dimension data to write")
    return()
  }
  
  dimension_names <- unique(unlist(map(all_dimensions, names)))
  
  # Write each dimension
  walk(dimension_names, function(dim_name) {
    write_single_dimension(all_dimensions, dim_name, output_dir)
  })
}

# Write a single dimension table
write_single_dimension <- function(all_dimensions, dim_name, output_dir) {
  # Collect all instances of this dimension
  dim_tables <- all_dimensions %>%
    map(dim_name) %>%
    compact()
  
  if (length(dim_tables) == 0) return()
  
  # Merge and deduplicate
  merged_dim <- rbindlist(dim_tables, fill = TRUE) %>%
    unique()
  
  # Write dimension
  dim_file <- file.path(output_dir, paste0(dim_name, ".parquet"))
  arrow::write_parquet(
    merged_dim,
    dim_file,
    compression = "snappy",
    use_dictionary = TRUE
  )
  
  log_audit(sprintf("Wrote %s: %d rows, %.2f MB", 
                    dim_name, 
                    nrow(merged_dim),
                    file.size(dim_file) / 1024^2))
}

# Create processing manifest
create_processing_manifest <- function(output_dir) {
  # Create manifest data
  manifest <- data.frame(
    timestamp = Sys.time(),
    processing_date = Sys.Date(),
    total_files = .total_files_processed,
    successful = .successful_files,
    failed = .failed_files,
    validation_failed = .validation_failed,
    success_rate = (.successful_files / .total_files_processed) * 100,
    duration_seconds = as.numeric(difftime(Sys.time(), .processing_start_time, units = "secs")),
    output_location = output_dir,
    batch_size = BATCH_SIZE,
    validation_enabled = ENABLE_VALIDATION
  )
  
  # Write manifest
  manifest_file <- file.path(output_dir, "processing_manifest.csv")
  
  write.table(
    manifest,
    manifest_file,
    append = file.exists(manifest_file),
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(manifest_file)
  )
  
  log_info(sprintf("Processing manifest written to %s", manifest_file))
}

# Create Parquet metadata
create_parquet_metadata <- function(output_dir) {
  # List all parquet files
  parquet_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  # Get metadata for each file
  metadata <- parquet_files %>%
    map_df(function(file) {
      pq <- arrow::read_parquet(file, as_data_frame = FALSE)
      data.frame(
        file = basename(file),
        rows = pq$num_rows,
        columns = pq$num_columns,
        size_mb = file.size(file) / 1024^2,
        compression = "snappy",
        created = Sys.time()
      )
    })
  
  # Write metadata
  write.csv(
    metadata,
    file.path(output_dir, "parquet_metadata.csv"),
    row.names = FALSE
  )
  
  # Print summary
  total_size <- sum(metadata$size_mb)
  total_rows <- sum(metadata$rows)
  
  log_info(sprintf("Total output: %d files, %.2f MB, %d rows", 
                   nrow(metadata), total_size, total_rows))
}