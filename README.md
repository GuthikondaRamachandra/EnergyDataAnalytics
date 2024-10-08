
# Data Extraction and Transformation Pipeline in Databricks

This pipeline extracts data from the UK Government’s website, performs transformations, and saves the result as a CSV file in the Databricks FileStore.
Note: Because of Resource Limitations used Databricks as end to end Implementation where as we can Implement this in Azure Data Factory with Databricks Integration to have additional features of ADF like schedululing,debug,logging etc
## Table of Contents
- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Setup Instructions](#setup-instructions)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Logging](#logging)
- [Final Output](#final-output)
- [Troubleshooting and Known Issues](#troubleshooting-and-known-issues)
- [Deployment to higher/production environment (In Future)](#deployment-to-higher-production-environment-in-future)
- [Best practices to be followed (In Future)](#best-practices-to-be-followed-in-future)

## Project Overview

This solution is a two-part pipeline consisting of:
1. **Ingestion**: Downloads energy trend data from the UK Government website, specifically Excel files, and saves them to Databricks DBFS.
2. **Transformation**: Processes the downloaded data, unpivots the data, and stores the transformed data as a CSV in the DBFS.

---

## Key Features

- **Resilient Data Downloading**: The retry mechanism for downloading files helps handle potential network or connection issues.
- **Data Transformation**: Converts quarterly data into a suitable format for analysis, renaming columns into `YYYYQQ` format, and adding metadata such as `FileName` and `ProcessedDate`.
- **Logging**: Provides detailed logging throughout the process for monitoring and troubleshooting.

---

## Setup Instructions

### Prerequisites
1. **Databricks Cluster**: Ensure that you have a running Databricks cluster with Python 3.9 or higher.
2. **Python Libraries**: Required libraries include `requests`, `pandas`, `beautifulsoup4`, and `pytest`.

   You can install these libraries in your Databricks notebook by running:

   ```python
   %pip install requests pandas beautifulsoup4 pytest
   ```

### Project Structure

```
.
├── EnergyTrendAnalysis.py               # Main pipeline code for extraction and transformation
├── EnergyTrendAnalysisTest.py           # Test pipeline code 
├── config.json               # Configuration file for URL and save path(at present its hardcoded - to be Implemeted)
└── README.md                 # Instructions and details about the solution
```

### Configuration

The pipeline reads some configuration variables from a JSON file (`config.json`). The configuration should look like this:

```json
{
  "url": "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends",
  "search_term": "Supply and use of crude oil",
  "latest_quarter": "1984 1st quarter",
  "excel_save_path": "/FileStore/tables/final_data_petroineous"
}
```

You can modify these settings based on the URL, search term, and file paths required for your specific project.

---

## Running the Pipeline

### Step 1: Clone the Code

Copy the code from the Databricks notebook or the provided Python scripts into a Databricks notebook.

### Step 2: Configure Parameters

Update the configuration file `config.json` to match the URL, search term, and other parameters you want to work with. This ensures that the correct data is fetched and saved.

### Step 3: Run the Ingestion Process

1. **Ingestion**: This part of the pipeline will download the required Excel file if newer data is available.

   ```python
   search_for_energy_trend(url, search_term, latest_quarter, excel_save_path)
   ```

2. **Transformation**: Once the data is downloaded, the pipeline will transform it into the desired format.

   ```python
   # Reading and transforming the data
   df_unpivot = df_renamed.select("Category", "Sub_Category", expr(f"stack({len(quarter_columns)}, {stack_expr}) as (Quarter, Quantity)"))
   finalDF = df_unpivot.withColumn("FileName", lit(file_name))                       .withColumn("ProcessedDate", lit(datetime.now().strftime('%d/%m/%Y')))
   ```

### Step 4: Check the Output

The final output is saved in the DBFS as a CSV file. You can view the file at the path specified in `csv_save_path`.

### Example Command to View Files:

```python
dbutils.fs.ls("/FileStore/tables/final_data_petroineous/")
```

---

## Logging

The pipeline uses Python’s built-in logging module to log important events. You can view logs directly in the notebook for real-time feedback on the process. The logs provide detailed information about:

- Download status of the Excel files.
- Status of transformations.
- Row count of the final DataFrame.

### Example Log Output:

```
2024-03-15 10:23:45,123 - INFO - Downloading file from https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends...
2024-03-15 10:23:46,543 - INFO - File downloaded successfully to temporary path: /tmp/file.xlsx
2024-03-15 10:23:46,765 - INFO - Checking if the file contains a newer quarter.
2024-03-15 10:23:47,012 - INFO - Newer quarter found: 2024 2nd quarter.
```

You can adjust the logging level by changing the configuration of the `logging` module.

---

## Final Output

The final transformed data is saved as a CSV file in the Databricks FileStore at `/FileStore/tables/final_data_petroineous/TransformedEnergyData.csv`.

You can download the file from the Databricks workspace or access it directly in your notebook using:

```python
dbutils.fs.cp("/FileStore/tables/final_data_petroineous/TransformedEnergyData.csv", "/local/path/to/save.csv")
```

---

## Troubleshooting and Known Issues

- **Retry Logic**: If a download fails due to network issues, the `retry_request` decorator will automatically retry up to 3 times with exponential backoff.
- **File Not Found**: If the Excel file link is not found, ensure the URL and search terms are correctly configured.
- **Memory Issues**: If dealing with large datasets, ensure your Databricks cluster has sufficient resources (memory, CPU).
- **Missing Columns**: If expected columns are not found in the Excel sheet, verify that the sheet structure matches the one expected by the pipeline.

## Deployment to higher/production environment(In Future)

We can have CI/CD Pipeline to deploy the pipelines by using Azure Devops, GIT, Databricks and scheduling can be done using Workflows or Azure Data Factory Or Airflow Or Apache NIFI


## Best practices to be followed (In Future)
1. Modular Code Structure : The project can be divided into separate modules
2. Config Files : use configuration file (config.json) and use it in the subsequest steps in pipelines
3. Docstrings: Each method and class are well-documented with docstrings explaining their purpose, parameters, and return values.
4. Version Control Integration: gitignore: A .gitignore file is used to exclude unnecessary files and directories from version control
5. Configuration via Environment Variables :  Flexibility: Instead of hardcoding configuration values, using environment variables or configuration files allows for easy changes without modifying the code
6. Code Quality : PEP 8 Compliance: The code follows PEP 8 style guidelines, ensuring readability and maintainability and Consistent Naming Conventions: Consistent and meaningful naming conventions for variables, functions, and classes improve code readability.
7. Data quality checks can be further implemented based on the requirements.
8. sonarQube can be integrated for the code quality.
9. Email actions for the failure and success jobs .
10. We can Integrate the Final Tables with Collibra to have Technical Metadata,Business Metadata and Operational Metadata



