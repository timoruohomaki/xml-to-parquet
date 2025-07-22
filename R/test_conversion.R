# test_conversion.R - Quick test of the converter

source("R/main.R")
source("R/xml_parser.R")
source("R/schema_analyzer.R")
source("R/star_transformer.R")
source("R/parquet_writer.R")
source("R/logger.R")
source("R/schema_validator.R")

# Copy examples to input folder
file.copy("examples/products.xml", "input/", overwrite = TRUE)
file.copy("examples/orders.xml", "input/", overwrite = TRUE)

# Run conversion
cat("Running test conversion...\n")
process_xml_to_parquet()

# Check results
if (file.exists("output/fact_main.parquet")) {
  cat("✓ Conversion successful!\n")
  fact_data <- arrow::read_parquet("output/fact_main.parquet")
  cat(sprintf("Fact table contains %d rows\n", nrow(fact_data)))
} else {
  cat("✗ Conversion failed - no output found\n")
}
