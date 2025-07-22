# schema_validator.R - XML Schema Validation Functions

# Validate XML against XSD schema
validate_xml_schema <- function(xml_file, schema_file = NULL) {
  # Use default schema if not provided
  if (is.null(schema_file)) {
    schema_file <- find_schema_file(xml_file, "xsd")
  }
  
  if (is.null(schema_file) || !file.exists(schema_file)) {
    return(list(
      valid = NA,
      file = xml_file,
      errors = "No XSD schema file found",
      schema_used = NA
    ))
  }
  
  tryCatch({
    # Read schema
    schema <- xml2::read_xml(schema_file)
    
    # Read and validate XML
    doc <- xml2::read_xml(xml_file)
    validation_result <- xml2::xml_validate(doc, schema)
    
    # Extract errors if validation failed
    errors <- if (!validation_result) {
      attr(validation_result, "errors")
    } else {
      character(0)
    }
    
    list(
      valid = validation_result,
      file = xml_file,
      errors = errors,
      schema_used = schema_file
    )
    
  }, error = function(e) {
    list(
      valid = FALSE,
      file = xml_file,
      errors = as.character(e),
      schema_used = schema_file
    )
  })
}

# Validate with DTD
validate_xml_dtd <- function(xml_file, dtd_file = NULL) {
  tryCatch({
    if (!is.null(dtd_file) && file.exists(dtd_file)) {
      # External DTD validation
      dtd <- xml2::read_xml(dtd_file, as_html = FALSE)
      doc <- xml2::read_xml(xml_file)
      validation_result <- xml2::xml_validate(doc, dtd)
      
      list(
        valid = validation_result,
        file = xml_file,
        errors = if (!validation_result) attr(validation_result, "errors") else character(0),
        schema_used = dtd_file
      )
    } else {
      # Try to parse with internal DTD validation
      doc <- xml2::read_xml(xml_file, options = c("DTDVALID", "NOBLANKS"))
      
      list(
        valid = TRUE,
        file = xml_file,
        errors = character(0),
        schema_used = "internal DTD"
      )
    }
  }, error = function(e) {
    list(
      valid = FALSE,
      file = xml_file,
      errors = as.character(e),
      schema_used = dtd_file
    )
  })
}

# Auto-detect validation method
validate_xml_auto <- function(xml_file, schema_dir = SCHEMA_DIR) {
  # First check if XML has internal DTD
  if (has_internal_dtd(xml_file)) {
    result <- validate_xml_dtd(xml_file)
    if (!is.na(result$valid)) return(result)
  }
  
  # Look for external schema files
  schema_file <- find_schema_file(xml_file, "xsd", schema_dir)
  if (!is.null(schema_file)) {
    return(validate_xml_schema(xml_file, schema_file))
  }
  
  dtd_file <- find_schema_file(xml_file, "dtd", schema_dir)
  if (!is.null(dtd_file)) {
    return(validate_xml_dtd(xml_file, dtd_file))
  }
  
  # No schema found
  list(
    valid = NA,
    file = xml_file,
    errors = "No schema found for validation",
    schema_used = NA
  )
}

# Find schema file for XML
find_schema_file <- function(xml_file, extension, schema_dir = SCHEMA_DIR) {
  base_name <- tools::file_path_sans_ext(basename(xml_file))
  
  # Search locations in order of preference
  search_paths <- c(
    # Specific schema for this file
    file.path(schema_dir, paste0(base_name, ".", extension)),
    # Schema in same directory as XML
    file.path(dirname(xml_file), paste0(base_name, ".", extension)),
    # Generic schema in schema directory
    file.path(schema_dir, paste0("schema.", extension)),
    # Generic schema in XML directory
    file.path(dirname(xml_file), paste0("schema.", extension)),
    # Default schema
    file.path(schema_dir, paste0("default.", extension))
  )
  
  # Return first existing file
  for (path in search_paths) {
    if (file.exists(path)) return(path)
  }
  
  NULL
}

# Check if XML has internal DTD
has_internal_dtd <- function(xml_file) {
  tryCatch({
    # Read first few lines to check for DOCTYPE
    lines <- readLines(xml_file, n = 10)
    any(grepl("<!DOCTYPE", lines, fixed = TRUE))
  }, error = function(e) FALSE)
}

# Batch validation
validate_xml_batch <- function(xml_files, schema_file = NULL, parallel = TRUE) {
  if (parallel) {
    validation_results <- future_map(
      xml_files,
      ~ validate_xml_auto(.x, schema_file),
      .progress = TRUE
    )
  } else {
    validation_results <- lapply(
      xml_files,
      function(x) validate_xml_auto(x, schema_file)
    )
  }
  
  # Summarize results
  summary <- list(
    total = length(xml_files),
    valid = sum(sapply(validation_results, function(x) isTRUE(x$valid))),
    invalid = sum(sapply(validation_results, function(x) isFALSE(x$valid))),
    no_schema = sum(sapply(validation_results, function(x) is.na(x$valid))),
    results = validation_results
  )
  
  # Log summary
  log_info(sprintf(
    "Batch validation complete: %d valid, %d invalid, %d no schema",
    summary$valid, summary$invalid, summary$no_schema
  ))
  
  summary
}

# Create validation report
create_validation_details <- function(validation_results, output_file) {
  # Convert results to data frame
  validation_df <- validation_results %>%
    map_df(function(x) {
      data.frame(
        file = basename(x$file),
        valid = ifelse(is.na(x$valid), "No Schema", ifelse(x$valid, "Valid", "Invalid")),
        schema_used = ifelse(is.na(x$schema_used), "None", basename(x$schema_used)),
        error = if (length(x$errors) > 0) x$errors[1] else "",
        stringsAsFactors = FALSE
      )
    })
  
  # Write detailed report
  write.csv(validation_df, output_file, row.names = FALSE)
  
  # Create summary
  summary_text <- validation_df %>%
    count(valid) %>%
    mutate(percentage = n / sum(n) * 100) %>%
    sprintf(fmt = "%s: %d files (%.1f%%)")
  
  writeLines(
    c("Validation Summary", "==================", summary_text),
    gsub("\\.csv$", "_summary.txt", output_file)
  )
  
  invisible(validation_df)
}