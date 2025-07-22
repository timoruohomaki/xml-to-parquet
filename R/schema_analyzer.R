# schema_analyzer.R - Schema Analysis Functions

# Analyze schema using functional approach
analyze_schema_functional <- function(sample_data) {
  # Get column information
  col_info <- sample_data %>%
    summarise(across(
      everything(),
      list(
        numeric_ratio = ~ {
          non_na <- .x[!is.na(.x)]
          if (length(non_na) == 0) return(0)
          sum(!is.na(suppressWarnings(as.numeric(non_na)))) / length(non_na)
        },
        unique_count = ~ n_distinct(.x, na.rm = TRUE),
        null_ratio = ~ sum(is.na(.x)) / n(),
        mean_length = ~ mean(nchar(as.character(.x)), na.rm = TRUE),
        sample_values = ~ paste(head(unique(.x), 3), collapse = "|")
      ),
      .names = "{.col}__{.fn}"
    )) %>%
    pivot_longer(everything()) %>%
    separate(name, c("column", "metric"), sep = "__") %>%
    pivot_wider(names_from = metric, values_from = value)
  
  # Classify columns
  schema_info <- col_info %>%
    mutate(
      classification = case_when(
        column == ID_ATTRIBUTE ~ "identifier",
        column == "record_id" ~ "identifier",
        column %in% c("source_file_name", "load_timestamp") ~ "audit",
        numeric_ratio > NUMERIC_THRESHOLD ~ "measure",
        unique_count < nrow(sample_data) * 0.1 & unique_count < 50 ~ "dimension",
        unique_count == nrow(sample_data) ~ "potential_key",
        TRUE ~ "attribute"
      ),
      data_type = case_when(
        numeric_ratio > 0.95 ~ "numeric",
        numeric_ratio > 0.5 ~ "mixed_numeric",
        mean_length > 100 ~ "text",
        TRUE ~ "string"
      )
    )
  
  # Log schema summary
  log_info("Schema analysis complete:")
  log_info(sprintf("  - Identifiers: %d", sum(schema_info$classification == "identifier")))
  log_info(sprintf("  - Measures: %d", sum(schema_info$classification == "measure")))
  log_info(sprintf("  - Dimensions: %d", sum(schema_info$classification == "dimension")))
  log_info(sprintf("  - Attributes: %d", sum(schema_info$classification == "attribute")))
  log_info(sprintf("  - Audit fields: %d", sum(schema_info$classification == "audit")))
  
  schema_info
}

# Get column type information
get_column_types <- function(data) {
  data %>%
    summarise(across(everything(), ~ class(.x)[1])) %>%
    pivot_longer(everything(), names_to = "column", values_to = "type")
}

# Detect numeric columns
detect_numeric_columns <- function(data) {
  numeric_ratios <- data %>%
    summarise(across(
      everything(),
      ~ sum(!is.na(suppressWarnings(as.numeric(.x)))) / n()
    )) %>%
    pivot_longer(everything(), names_to = "column", values_to = "numeric_ratio")
  
  numeric_ratios %>%
    filter(numeric_ratio > NUMERIC_THRESHOLD) %>%
    pull(column)
}

# Detect dimension candidates
detect_dimension_columns <- function(data, schema_info) {
  schema_info %>%
    filter(
      classification == "dimension" |
        (unique_count < 50 & classification != "measure")
    ) %>%
    pull(column)
}

# Validate schema for star schema transformation
validate_star_schema <- function(schema_info) {
  measures <- schema_info %>%
    filter(classification == "measure") %>%
    pull(column)
  
  dimensions <- schema_info %>%
    filter(classification == "dimension") %>%
    pull(column)
  
  if (length(measures) == 0) {
    log_warning("No measure columns detected - fact table will be empty")
  }
  
  if (length(dimensions) == 0) {
    log_warning("No dimension columns detected - no dimension tables will be created")
  }
  
  list(
    valid = length(measures) > 0 | length(dimensions) > 0,
    measures = measures,
    dimensions = dimensions
  )
}

# Create schema documentation
document_schema <- function(schema_info, output_file) {
  # Create readable schema documentation
  schema_doc <- schema_info %>%
    select(column, classification, data_type, unique_count, null_ratio, sample_values) %>%
    arrange(classification, column)
  
  # Write documentation
  write.csv(schema_doc, output_file, row.names = FALSE)
  
  # Create summary
  summary_text <- sprintf(
    "Schema Documentation\n" %s+%
      "==================\n" %s+%
      "Total Columns: %d\n" %s+%
      "Measures: %s\n" %s+%
      "Dimensions: %s\n" %s+%
      "Identifiers: %s\n",
    nrow(schema_info),
    paste(schema_info$column[schema_info$classification == "measure"], collapse = ", "),
    paste(schema_info$column[schema_info$classification == "dimension"], collapse = ", "),
    paste(schema_info$column[schema_info$classification == "identifier"], collapse = ", ")
  )
  
  writeLines(summary_text, gsub("\\.csv$", "_summary.txt", output_file))
  
  invisible(schema_doc)
}