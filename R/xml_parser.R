# xml_parser.R - XML Parsing Functions



# Parse XML with validation and logging
parse_xml_with_validation <- function(file_path, validate = TRUE, schema_file = NULL) {
  # Step 1: Validate if requested
  if (validate) {
    validation_result <- if (!is.null(schema_file)) {
      validate_xml_schema(file_path, schema_file)
    } else {
      validate_xml_auto(file_path)
    }
    
    # Log validation result
    if (!is.na(validation_result$valid)) {
      if (validation_result$valid) {
        log_message(
          sprintf("VALIDATED: %s against %s", 
                  basename(file_path), 
                  basename(validation_result$schema_used)),
          AUDIT_LOG,
          "SCHEMA"
        )
      } else {
        log_message(
          sprintf("VALIDATION FAILED: %s - %s", 
                  basename(file_path), 
                  validation_result$errors[1]),
          ERROR_LOG,
          "SCHEMA"
        )
        
        # Return empty result for invalid files
        return(list(
          status = "validation_error",
          file = file_path,
          data = data.table(),
          rows = 0,
          error = paste("Schema validation failed:", validation_result$errors[1])
        ))
      }
    }
  }
  
  # Step 2: Parse if valid or validation skipped
  parse_xml_with_logging(file_path)
}

# Parse XML with detailed logging
parse_xml_with_logging <- function(file_path) {
  log_file_start(file_path)
  
  result <- tryCatch({
    # Parse the file
    data <- parse_xml_streaming(file_path)
    
    # Add source file reference - this will be preserved in fact table
    if (nrow(data) > 0) {
      data[, source_file_name := basename(file_path)]
      data[, source_file_path := file_path]
      data[, load_timestamp := Sys.time()]
    }
    
    log_file_success(file_path, nrow(data))
    
    list(
      status = "success",
      file = file_path,
      data = data,
      rows = nrow(data),
      error = NULL
    )
    
  }, error = function(e) {
    error_msg <- trimws(as.character(e))
    log_file_error(file_path, error_msg)
    
    list(
      status = "error",
      file = file_path,
      data = data.table(),
      rows = 0,
      error = error_msg
    )
  })
  
  return(result)
}

# Memory-efficient streaming parser
parse_xml_streaming <- function(file_path) {
  # Read with minimal memory footprint
  xml_doc <- xml2::read_xml(file_path, options = c("NOBLANKS", "HUGE"))
  
  result <- tryCatch({
    # Find all record nodes - adjust xpath as needed
    nodes <- xml2::xml_find_all(xml_doc, ".//record | .//Record | .//item | .//Item")
    
    if (length(nodes) == 0) {
      # Try root children if no records found
      root <- xml2::xml_root(xml_doc)
      nodes <- xml2::xml_children(root)
    }
    
    if (length(nodes) == 0) {
      return(data.table())
    }
    
    # Process nodes in chunks
    chunk_size <- 1000
    node_chunks <- split(nodes, ceiling(seq_along(nodes) / chunk_size))
    
    # Process each chunk
    chunk_results <- node_chunks %>%
      lapply(process_node_chunk) %>%
      rbindlist(fill = TRUE)
    
    chunk_results
    
  }, error = function(e) {
    log_error(sprintf("Error parsing %s: %s", file_path, e$message))
    data.table()
  })
  
  # Clean up immediately
  rm(xml_doc)
  gc(verbose = FALSE)
  
  result
}

# Process a chunk of nodes
process_node_chunk <- function(nodes) {
  # Extract base data
  base_data <- data.table(
    record_id = xml2::xml_attr(nodes, ID_ATTRIBUTE, default = NA_character_)
  )
  
  # If no ID attribute, generate sequential IDs
  if (all(is.na(base_data$record_id))) {
    base_data$record_id <- seq_len(nrow(base_data))
  }
  
  # Extract all attributes
  all_attrs <- nodes %>%
    lapply(function(node) {
      attrs <- xml2::xml_attrs(node)
      if (length(attrs) > 0) as.list(attrs) else list()
    }) %>%
    rbindlist(fill = TRUE)
  
  # Extract child elements
  child_data <- nodes %>%
    lapply(extract_child_elements) %>%
    rbindlist(fill = TRUE)
  
  # Extract text content if nodes have direct text
  text_content <- xml2::xml_text(nodes, trim = TRUE)
  if (any(nzchar(text_content))) {
    base_data$text_content <- text_content
  }
  
  # Combine all data
  if (ncol(all_attrs) > 0) base_data <- cbind(base_data, all_attrs)
  if (ncol(child_data) > 0) base_data <- cbind(base_data, child_data)
  
  base_data
}

# Extract child elements recursively
extract_child_elements <- function(node) {
  children <- xml2::xml_children(node)
  if (length(children) == 0) return(list())
  
  # Get child names and values
  child_names <- xml2::xml_name(children)
  
  # Process each child
  child_values <- children %>%
    lapply(function(child) {
      # Check if child has children
      grandchildren <- xml2::xml_children(child)
      if (length(grandchildren) == 0) {
        # Leaf node - get text
        xml2::xml_text(child, trim = TRUE)
      } else {
        # Has children - concatenate their values
        paste(xml2::xml_text(grandchildren, trim = TRUE), collapse = " ")
      }
    })
  
  # Handle duplicate names by making them unique
  if (any(duplicated(child_names))) {
    child_names <- make.unique(child_names)
  }
  
  setNames(child_values, child_names)
}

# Analyze schema from sample files
analyze_schema_from_files <- function(xml_files) {
  log_info(sprintf("Analyzing schema from %d sample files", length(xml_files)))
  
  # Parse sample files
  sample_data <- xml_files %>%
    future_map(~ {
      result <- parse_xml_with_logging(.x)
      if (result$status == "success") result$data else NULL
    }, .progress = FALSE) %>%
    compact() %>%
    rbindlist(fill = TRUE)
  
  if (nrow(sample_data) == 0) {
    log_error("No data extracted from sample files")
    return(data.table())
  }
  
  # Remove metadata columns
  sample_data[, c("source_file", "file_path") := NULL]
  
  # Analyze schema
  analyze_schema_functional(sample_data)
}

# Extract business key from XML comment
extract_business_key_from_comment <- function(xml_doc) {
  tryCatch({
    # Find all comment nodes in the document
    comments <- xml2::xml_find_all(xml_doc, "//comment()")
    
    if (length(comments) == 0) {
      return(list(found = FALSE))
    }
    
    # Process first comment only (as per requirement)
    comment_text <- xml2::xml_text(comments[1])
    
    # Pattern: ABCD:1234
    pattern <- "^([A-Za-z]+):([^:]+)$"
    
    if (grepl(pattern, trimws(comment_text))) {
      matches <- regmatches(trimws(comment_text), regexec(pattern, trimws(comment_text)))[[1]]
      
      return(list(
        found = TRUE,
        attribute_name = matches[2],
        attribute_value = matches[3],
        raw_comment = comment_text
      ))
    }
    
    # Comment exists but doesn't match pattern
    log_warning(sprintf("Comment found but doesn't match pattern: %s", comment_text))
    return(list(found = FALSE, raw_comment = comment_text))
    
  }, error = function(e) {
    log_warning(sprintf("Error extracting comment: %s", e$message))
    return(list(found = FALSE))
  })
}

# Enhanced streaming parser with comment support
parse_xml_streaming_with_comments <- function(file_path) {
  # Read with minimal memory footprint
  xml_doc <- xml2::read_xml(file_path, options = c("NOBLANKS", "HUGE"))
  
  # Extract business key from comment FIRST
  business_key_info <- extract_business_key_from_comment(xml_doc)
  
  result <- tryCatch({
    # Find all record nodes
    nodes <- xml2::xml_find_all(xml_doc, ".//record | .//Record | .//item | .//Item")
    
    if (length(nodes) == 0) {
      root <- xml2::xml_root(xml_doc)
      nodes <- xml2::xml_children(root)
    }
    
    if (length(nodes) == 0) {
      return(data.table())
    }
    
    # Process nodes in chunks
    chunk_size <- 1000
    node_chunks <- split(nodes, ceiling(seq_along(nodes) / chunk_size))
    
    # Process each chunk
    chunk_results <- node_chunks %>%
      lapply(process_node_chunk) %>%
      rbindlist(fill = TRUE)
    
    # Add business key if found
    if (business_key_info$found) {
      # Dynamic column name from comment
      chunk_results[, (business_key_info$attribute_name) := business_key_info$attribute_value]
      
      # Also store as standardized columns for tracking
      chunk_results[, business_key_name := business_key_info$attribute_name]
      chunk_results[, business_key_value := business_key_info$attribute_value]
      
      # Log successful extraction
      log_audit(sprintf("Extracted business key from %s: %s=%s", 
                        basename(file_path),
                        business_key_info$attribute_name,
                        business_key_info$attribute_value))
    }
    
    chunk_results
    
  }, error = function(e) {
    log_error(sprintf("Error parsing %s: %s", file_path, e$message))
    data.table()
  })
  
  # Clean up immediately
  rm(xml_doc)
  gc(verbose = FALSE)
  
  result
}

# Update the main parsing function to use enhanced version
parse_xml_with_logging <- function(file_path) {
  log_file_start(file_path)
  
  result <- tryCatch({
    # Use enhanced parser with comment support
    data <- parse_xml_streaming_with_comments(file_path)
    
    # Add source file reference
    if (nrow(data) > 0) {
      data[, source_file_name := basename(file_path)]
      data[, source_file_path := file_path]
      data[, load_timestamp := Sys.time()]
    }
    
    log_file_success(file_path, nrow(data))
    
    list(
      status = "success",
      file = file_path,
      data = data,
      rows = nrow(data),
      error = NULL
    )
    
  }, error = function(e) {
    error_msg <- trimws(as.character(e))
    log_file_error(file_path, error_msg)
    
    list(
      status = "error",
      file = file_path,
      data = data.table(),
      rows = 0,
      error = error_msg
    )
  })
  
  return(result)
}