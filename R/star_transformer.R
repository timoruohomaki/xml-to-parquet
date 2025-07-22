# star_transformer.R - Star Schema Transformation Functions

# Build star schema from data
build_star_schema <- function(data, schema_info) {
  if (nrow(data) == 0 || nrow(schema_info) == 0) {
    return(list(fact = data.table(), dimensions = list()))
  }
  
  # Get column classifications
  measure_cols <- schema_info %>%
    filter(classification == "measure") %>%
    pull(column)
  
  dimension_cols <- schema_info %>%
    filter(classification == "dimension") %>%
    pull(column)
  
  identifier_cols <- schema_info %>%
    filter(classification %in% c("identifier", "potential_key")) %>%
    pull(column)
  
  audit_cols <- schema_info %>%
    filter(classification == "audit") %>%
    pull(column)
  
  # Ensure we have the primary identifier
  if (!ID_ATTRIBUTE %in% names(data) && !"record_id" %in% names(data)) {
    data$record_id <- seq_len(nrow(data))
    identifier_cols <- c("record_id", identifier_cols)
  }
  
  # Build dimension tables functionally
  dimensions <- dimension_cols %>%
    lapply(function(col) {
      build_dimension_table(data, col, identifier_cols[1])
    }) %>%
    setNames(paste0(DIM_PREFIX, dimension_cols))
  
  # Build fact table with audit columns
  fact_table <- build_fact_table(data, measure_cols, dimensions, identifier_cols[1])
  
  list(
    fact = fact_table,
    dimensions = dimensions
  )
}

# Build a single dimension table
build_dimension_table <- function(data, dim_column, id_column) {
  # Select unique combinations
  dim_data <- data %>%
    select(all_of(c(id_column, dim_column))) %>%
    filter(!is.na(!!sym(dim_column))) %>%
    select(all_of(dim_column)) %>%
    distinct() %>%
    arrange(!!sym(dim_column))
  
  # Add surrogate key
  dim_data <- dim_data %>%
    mutate(!!paste0(dim_column, "_key") := row_number()) %>%
    select(!!paste0(dim_column, "_key"), everything())
  
  # Add metadata
  dim_data <- dim_data %>%
    mutate(
      created_date = Sys.Date(),
      is_active = TRUE
    )
  
  as.data.table(dim_data)
}

# Build fact table with dimension keys
build_fact_table <- function(data, measure_cols, dimension_tables, id_column) {
  # Define audit columns to preserve
  audit_columns <- c("source_file_name", "source_file_path", "load_timestamp")
  existing_audit_cols <- intersect(audit_columns, names(data))
  
  # Start with identifiers, measures, and audit columns
  available_measures <- intersect(measure_cols, names(data))
  
  if (length(available_measures) == 0) {
    # No measures, create count as default measure
    fact_data <- data %>%
      select(all_of(c(id_column, existing_audit_cols))) %>%
      mutate(record_count = 1)
  } else {
    fact_data <- data %>%
      select(all_of(c(id_column, available_measures, existing_audit_cols)))
  }
  
  # Convert measures to numeric
  fact_data <- fact_data %>%
    mutate(across(
      all_of(available_measures),
      ~ suppressWarnings(as.numeric(.x))
    ))
  
  # Add dimension keys using functional approach
  if (length(dimension_tables) > 0) {
    fact_data <- reduce(
      names(dimension_tables),
      function(fact, dim_name) {
        add_dimension_key(fact, data, dimension_tables[[dim_name]], dim_name, id_column)
      },
      .init = fact_data
    )
  }
  
  # Add processing metadata
  fact_data <- fact_data %>%
    mutate(
      load_date = Sys.Date(),
      load_time = format(Sys.time(), "%H:%M:%S"),
      batch_id = sample(1:1000000, 1)  # Random batch ID for this load
    )
  
  # Remove dimension value columns, keep only keys and audit columns
  dim_value_cols <- gsub(paste0("^", DIM_PREFIX), "", names(dimension_tables))
  fact_data <- fact_data %>%
    select(-any_of(setdiff(dim_value_cols, audit_columns)))
  
  as.data.table(fact_data)
}

# Add dimension key to fact table
add_dimension_key <- function(fact_data, original_data, dim_table, dim_name, id_column) {
  # Extract dimension column name
  dim_col <- gsub(paste0("^", DIM_PREFIX), "", dim_name)
  key_col <- paste0(dim_col, "_key")
  
  # Get mapping from original data
  dim_mapping <- original_data %>%
    select(all_of(c(id_column, dim_col))) %>%
    distinct() %>%
    left_join(
      dim_table %>% select(all_of(c(dim_col, key_col))),
      by = dim_col
    ) %>%
    select(all_of(c(id_column, key_col)))
  
  # Join with fact table
  fact_data %>%
    left_join(dim_mapping, by = id_column)
}

# Aggregate fact data by dimensions
aggregate_fact_data <- function(fact_data, group_by_cols, measure_cols) {
  fact_data %>%
    group_by(across(all_of(group_by_cols))) %>%
    summarise(
      across(
        all_of(measure_cols),
        list(
          sum = ~ sum(.x, na.rm = TRUE),
          avg = ~ mean(.x, na.rm = TRUE),
          min = ~ min(.x, na.rm = TRUE),
          max = ~ max(.x, na.rm = TRUE),
          count = ~ sum(!is.na(.x))
        ),
        .names = "{.col}_{.fn}"
      ),
      .groups = "drop"
    )
}

# Validate star schema integrity
validate_star_schema_integrity <- function(star_schema) {
  issues <- list()
  
  # Check fact table
  if (nrow(star_schema$fact) == 0) {
    issues <- append(issues, "Fact table is empty")
  }
  
  # Check each dimension
  for (dim_name in names(star_schema$dimensions)) {
    dim_table <- star_schema$dimensions[[dim_name]]
    
    if (nrow(dim_table) == 0) {
      issues <- append(issues, sprintf("Dimension %s is empty", dim_name))
    }
    
    # Check for duplicate keys
    key_col <- grep("_key$", names(dim_table), value = TRUE)[1]
    if (any(duplicated(dim_table[[key_col]]))) {
      issues <- append(issues, sprintf("Duplicate keys in dimension %s", dim_name))
    }
  }
  
  if (length(issues) > 0) {
    walk(issues, ~ log_warning(sprintf("Star schema issue: %s", .x)))
  }
  
  list(
    valid = length(issues) == 0,
    issues = issues
  )
}