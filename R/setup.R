# setup.R - Setup script for XML to Parquet converter

cat("Setting up XML to Parquet converter...\n\n")

# Check and install required packages
required_packages <- c(
  "xml2",
  "arrow", 
  "data.table",
  "future",
  "furrr",
  "magrittr",
  "tidyr",
  "dplyr",
  "purrr"
)

# Function to check and install packages
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("Installing %s...\n", pkg))
    install.packages(pkg)
  } else {
    cat(sprintf("✓ %s is already installed\n", pkg))
  }
}

# Install packages
cat("Checking required packages:\n")
invisible(lapply(required_packages, install_if_missing))

# Create directory structure
cat("\nCreating directory structure...\n")
dirs <- c("input", "output", "logs", "schemas", "examples", "tests")

for (dir in dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    cat(sprintf("Created directory: %s\n", dir))
  } else {
    cat(sprintf("✓ Directory exists: %s\n", dir))
  }
}

# Create example XML files
cat("\nCreating example XML files...\n")

# Example 1: Simple product catalog
example1 <- '<?xml version="1.0" encoding="UTF-8"?>
<products>
  <record id="1" category="electronics" brand="Samsung">
    <name>Galaxy S21</name>
    <price>799.99</price>
    <quantity>50</quantity>
    <rating>4.5</rating>
  </record>
  <record id="2" category="electronics" brand="Apple">
    <name>iPhone 13</name>
    <price>899.99</price>
    <quantity>45</quantity>
    <rating>4.7</rating>
  </record>
  <record id="3" category="accessories" brand="Samsung">
    <name>Wireless Charger</name>
    <price>49.99</price>
    <quantity>100</quantity>
    <rating>4.2</rating>
  </record>
</products>'

writeLines(example1, "examples/products.xml")

# Example 2: Orders with nested structure
example2 <- '<?xml version="1.0" encoding="UTF-8"?>
<orders>
  <record id="1001" customer="ABC Corp" region="North">
    <order_date>2024-01-15</order_date>
    <items>
      <total_amount>2549.97</total_amount>
      <item_count>3</item_count>
      <discount>127.50</discount>
    </items>
    <status>completed</status>
  </record>
  <record id="1002" customer="XYZ Ltd" region="South">
    <order_date>2024-01-16</order_date>
    <items>
      <total_amount>1299.99</total_amount>
      <item_count>1</item_count>
      <discount>0</discount>
    </items>
    <status>pending</status>
  </record>
</orders>'

writeLines(example2, "examples/orders.xml")

# Create example XSD schema
schema_example <- '<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="products">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="record" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="name" type="xs:string"/>
              <xs:element name="price" type="xs:decimal"/>
              <xs:element name="quantity" type="xs:integer"/>
              <xs:element name="rating" type="xs:decimal" minOccurs="0"/>
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="required"/>
            <xs:attribute name="category" type="xs:string"/>
            <xs:attribute name="brand" type="xs:string"/>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'

writeLines(schema_example, "schemas/products.xsd")

# Test the setup
cat("\nTesting setup...\n")

# Load libraries
suppressPackageStartupMessages({
  library(xml2)
  library(arrow)
  library(data.table)
  library(future)
})

# Test XML reading
tryCatch({
  test_xml <- read_xml("examples/products.xml")
  cat("✓ XML parsing works\n")
}, error = function(e) {
  cat("✗ XML parsing failed:", e$message, "\n")
})

# Test parallel processing
tryCatch({
  plan(multisession, workers = 2)
  test_result <- future_map(1:2, ~ .x * 2)
  plan(sequential)
  cat("✓ Parallel processing works\n")
}, error = function(e) {
  cat("✗ Parallel processing failed:", e$message, "\n")
})

# Create a simple test script
test_script <- '# test_conversion.R - Quick test of the converter

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
cat("Running test conversion...\\n")
process_xml_to_parquet()

# Check results
if (file.exists("output/fact_main.parquet")) {
  cat("✓ Conversion successful!\\n")
  fact_data <- arrow::read_parquet("output/fact_main.parquet")
  cat(sprintf("Fact table contains %d rows\\n", nrow(fact_data)))
} else {
  cat("✗ Conversion failed - no output found\\n")
}'

writeLines(test_script, "test_conversion.R")

cat("\n✓ Setup complete!\n\n")
cat("To test the converter, run:\n")
cat("  source('test_conversion.R')\n\n")
cat("To use the converter with your own data:\n")
cat("  1. Place XML files in the 'input' folder\n")
cat("  2. Optionally add XSD schemas to the 'schemas' folder\n")
cat("  3. Run: source('R/main.R') then process_xml_to_parquet()\n")