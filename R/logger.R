# logger.R - Logging Functions

# Core logging function
log_message <- function(message, file, level) {
  # Ensure log directory exists
  log_dir <- dirname(file)
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  # Thread-safe write
  cat(
    sprintf("[%s] [%s] %s\n", 
            format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
            level,
            message),
    file = file,
    append = TRUE
  )
}

# Convenience functions
log_info <- function(message) {
  log_message(message, AUDIT_LOG, "INFO")
  message(message)  # Also print to console
}

log_error <- function(message) {
  log_message(message, ERROR_LOG, "ERROR")
  log_message(message, AUDIT_LOG, "ERROR")  # Also log to audit
  warning(message)  # Also print warning
}

log_warning <- function(message) {
  log_message(message, AUDIT_LOG, "WARN")
  message(paste("Warning:", message))
}

log_audit <- function(message) {
  log_message(message, AUDIT_LOG, "AUDIT")
}

# File-specific logging
log_file_start <- function(file_path) {
  log_message(
    sprintf("Processing file: %s", basename(file_path)),
    AUDIT_LOG,
    "START"
  )
}

log_file_success <- function(file_path, rows_extracted) {
  log_message(
    sprintf("SUCCESS: %s - Extracted %d rows", basename(file_path), rows_extracted),
    AUDIT_LOG,
    "COMPLETE"
  )
}

log_file_error <- function(file_path, error_msg) {
  # Log to both error and audit logs
  msg <- sprintf("FAILED: %s - %s", basename(file_path), error_msg)
  log_message(msg, ERROR_LOG, "ERROR")
  log_message(msg, AUDIT_LOG, "ERROR")
}

# Batch logging
log_batch_summary <- function(batch_id, total_files, successful, failed) {
  summary_msg <- sprintf(
    "Batch %d complete: %d/%d successful, %d failed",
    batch_id, successful, total_files, failed
  )
  log_message(summary_msg, AUDIT_LOG, "BATCH")
}

# Processing report
create_processing_report <- function() {
  duration <- difftime(Sys.time(), .processing_start_time, units = "secs")
  success_rate <- ifelse(.total_files_processed > 0,
                         (.successful_files / .total_files_processed) * 100,
                         0)
  
  report <- sprintf(
    "\n========== PROCESSING SUMMARY ==========\n" %s+%
      "Start Time: %s\n" %s+%
      "End Time: %s\n" %s+%
      "Duration: %.1f seconds\n" %s+%
      "Total Files Processed: %d\n" %s+%
      "Successful: %d\n" %s+%
      "Failed: %d\n" %s+%
      "Success Rate: %.1f%%\n" %s+%
      "=======================================\n",
    .processing_start_time,
    Sys.time(),
    duration,
    .total_files_processed,
    .successful_files,
    .failed_files,
    success_rate
  )
  
  cat(report, file = AUDIT_LOG, append = TRUE)
  message(report)
}

# Validation report
create_validation_report <- function(output_dir) {
  validation_rate <- ifelse(
    .validated_files + .validation_failed > 0,
    (.validated_files / (.validated_files + .validation_failed)) * 100,
    NA
  )
  
  validation_summary <- data.frame(
    timestamp = Sys.time(),
    total_files = .total_files_processed,
    validated = .validated_files,
    validation_failed = .validation_failed,
    validation_skipped = .total_files_processed - .validated_files - .validation_failed,
    validation_rate = validation_rate,
    schema_dir = SCHEMA_DIR,
    validation_mode = VALIDATION_MODE
  )
  
  # Write report
  report_file <- file.path(output_dir, "validation_report.csv")
  write.csv(validation_summary, report_file, row.names = FALSE)
  
  # Log summary
  if (!is.na(validation_rate)) {
    log_info(sprintf(
      "Validation Summary: %d/%d files passed schema validation (%.1f%%)",
      .validated_files,
      .validated_files + .validation_failed,
      validation_rate
    ))
  }
}

# Memory usage logging
log_memory_usage <- function(context = "") {
  gc_info <- gc()
  used_mb <- sum(gc_info[, "used"]) / 1024
  max_mb <- sum(gc_info[, "max used"]) / 1024
  
  log_message(
    sprintf("Memory usage%s: %.1f MB used, %.1f MB max",
            ifelse(nzchar(context), paste0(" (", context, ")"), ""),
            used_mb, max_mb),
    AUDIT_LOG,
    "MEMORY"
  )
}

# Performance timing
log_timing <- function(start_time, operation) {
  elapsed <- difftime(Sys.time(), start_time, units = "secs")
  log_message(
    sprintf("Operation '%s' completed in %.2f seconds", operation, elapsed),
    AUDIT_LOG,
    "PERF"
  )
}

# Helper for string concatenation
`%s+%` <- function(x, y) paste0(x, y)