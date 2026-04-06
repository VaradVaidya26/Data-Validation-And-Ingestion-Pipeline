# Data-Validation-And-Ingestion-Pipeline

## Overview

This project implements an end-to-end data validation and ingestion pipeline using Azure Data Lake, Azure SQL, and Databricks (PySpark).

The pipeline processes incoming CSV files, applies strict validation rules (including schema validation from existing data), and ensures only high-quality data is loaded into Delta Tables for downstream analytics.

## Architecture Summary

<img width="1572" height="703" alt="Screenshot 2026-04-06 145745" src="https://github.com/user-attachments/assets/f64b2dfc-df69-4283-90c7-173cbae7b851" />

- Landing Layer: Raw CSV files are ingested into Azure Data Lake
- Processing Layer (Databricks):
- Data validation
- Schema enforcement
- Metadata Layer (Azure SQL):
Stores expected schema (column names & data types)
Stores date format rules
- Output Layers:
Staging → Valid files
Rejected → Invalid files

## Key Features

1. Data Ingestion
CSV files are received from internal applications
Stored in Azure Data Lake (Landing folder)

2. Data Validation Rules
- Duplicate Check
Ensures no duplicate rows exist
If duplicates are found → file is rejected
- Schema Validation
Incoming file schema is validated against existing schema stored in Azure SQL
Ensures:
Column names match
Data types are consistent
Prevents schema drift in the pipeline
If schema mismatch occurs → file is rejected
- Date Format Validation
Validates all date columns
Date formats are dynamically fetched from Azure SQL
If format mismatch occurs → file is rejected

3. File Routing Logic
Condition	Destination
Validation Passed	Staging Folder
Validation Failed	Rejected Folder

4. Data Storage
Validated data is written as Delta Tables in Databricks
Enables:
- ACID transactions
- Scalable data lakehouse architecture
- Efficient querying

## Tech Stack
Azure Data Lake Storage (ADLS) – Raw & processed data storage
Azure Databricks (PySpark) – Data validation & transformation
Azure SQL Database – Metadata store (schema + date formats)
Python / PySpark – Core pipeline logic

## Pipeline Flow
File arrives in Landing -> Databricks job reads the file -> Fetch expected schema & rules from Azure SQL -> Apply validations: Duplicate check, Schema validation (against existing data), Date format validation -> Based on result: Move to Staging/Move to Rejected -> Write valid data to Delta Table

## Sample Validation Logic (PySpark)

Duplicate Check
total_count = df.count()
distinct_count = df.distinct().count()

if total_count != distinct_count:
    errorFlag = True
    errorMessage = "Duplicate records found"
Schema Validation (Concept)
expected_columns = get_schema_from_azure_sql()
incoming_columns = df.columns

if expected_columns != incoming_columns:
    errorFlag = True
    errorMessage = "Schema mismatch"

## Key Highlights
- Metadata-driven pipeline using Azure SQL
- Prevents schema drift and bad data ingestion
- Built on Lakehouse architecture (Delta Tables)
- Ensures data quality before ingestion
