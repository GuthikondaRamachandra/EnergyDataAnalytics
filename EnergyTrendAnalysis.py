# Databricks notebook source
# MAGIC %md
# MAGIC Ingestion

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import time
from requests.exceptions import RequestException
import shutil
import logging

# COMMAND ----------

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a retry decorator for handling retries
def retry_request(max_retries=3, delay=5, backoff=2):
    """
    A decorator to retry a function on failure with exponential backoff.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    retries += 1
                    logging.error(f"Error: {e}. Retrying {retries}/{max_retries} after {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff  # Increase delay exponentially
            logging.error(f"Failed to complete after {max_retries} retries.")
            raise Exception(f"Failed to complete after {max_retries} retries.")
        return wrapper
    return decorator

# COMMAND ----------

@retry_request(max_retries=3, delay=5, backoff=2)
def download_excel_file(url, dbfs_save_path, file_name):
    """
    Downloads an Excel file from the provided URL and saves it to a temporary path.
    Then moves the file to the final DBFS path.
    """
    try:
        logging.info(f"Downloading file from {url}...")

        # Temporary local path in /tmp directory
        temp_save_path = f'/tmp/{file_name}'  

        # Making the request to download the file
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        # Write the file to local /tmp directory
        with open(temp_save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        
        logging.info(f"File downloaded successfully to temporary path: {temp_save_path}")

        # Now move the file to DBFS using Databricks utilities
        final_dbfs_path = f'{dbfs_save_path}/{file_name}'  # Keep as dbfs:/ path

        # Copy from local /tmp to DBFS path using dbutils.fs.cp
        dbutils.fs.cp(f"file:{temp_save_path}", final_dbfs_path)
        logging.info(f"File moved successfully to DBFS path: {final_dbfs_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during file download: {str(e)}")
        raise  # Rethrow the exception to trigger a retry
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


# COMMAND ----------

def quarter_to_tuple(quarter_str):
    """
    Convert a quarter string like '2024 1st quarter' into a tuple (2024, 1).
    """
    match = re.match(r'(\d{4}) (\d{1,2})(st|nd|rd|th) quarter', quarter_str)
    if match:
        year = int(match.group(1))
        quarter_number = int(match.group(2))
        # logging.info(f"Converted '{quarter_str}' to tuple ({year}, {quarter_number})")
        return (year, quarter_number)
    else:
        logging.error(f"Invalid quarter format: {quarter_str}")
        raise ValueError(f"Invalid quarter format: {quarter_str}")

# COMMAND ----------


def check_excel_for_latest_quarter(excel_url, latest_quarter, save_path, file_name):
    """
    Check the Excel file for columns representing quarter information (e.g., '2024 1st quarter').
    If the file contains a newer quarter than the latest_quarter, return True and the latest quarter value.
    """
    try:
        # Download the Excel file temporarily for inspection
        temp_excel_path = f'/tmp/{file_name}' 
        logging.info(f"Downloading Excel file from {excel_url} to {temp_excel_path} for inspection.")
        download_excel_file(excel_url, temp_excel_path, file_name)

        # Read the Excel file using Pandas (reading the 'Quarter' tab)
        df = pd.read_excel(temp_excel_path, sheet_name='Quarter', header=4)
        df.columns = df.columns.str.strip().str.replace('\n', ' ', regex=False)

        # logging.info(f"Excel file read successfully. Columns: {list(df.columns)}")

        # Extract all column names related to quarters
        quarter_columns = [col for col in df.columns if re.match(r'^\d{4} \d{1,2}(st|nd|rd|th) quarter', col)]
        # logging.info(f"Quarter columns found: {quarter_columns}")

        # Normalize the latest quarter for comparison by stripping any additional text
        normalized_latest_quarter = latest_quarter.strip()

        # Compare the latest quarter
        latest_quarter_in_file = max(quarter_columns, key=lambda x: quarter_to_tuple(re.sub(r'\s*\[.*\]', '', x)))

        logging.info(f"Latest quarter in the file: {latest_quarter_in_file}")

        # Convert quarters to a comparable format (tuple of year, quarter number)
        if quarter_to_tuple(latest_quarter_in_file) > quarter_to_tuple(normalized_latest_quarter):
            # New quarter is available
            logging.info(f"Newer quarter found: {latest_quarter_in_file}. Removing temporary file.")
            os.remove(temp_excel_path)  # Clean up temp file
            return True, latest_quarter_in_file

        # No newer data found, clean up
        logging.info("No newer data found. Cleaning up temporary file.")
        os.remove(temp_excel_path)
        return False, latest_quarter

    except Exception as e:
        logging.error(f"Error occurred during Excel inspection: {e}")
        return False, latest_quarter


# COMMAND ----------

def search_for_energy_trend(url, search_term, latest_quarter, excel_save_path):
    try:
        # Fetch the content of the webpage
        logging.info(f"Fetching content from URL: {url}")
        response = requests.get(url)
        
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            logging.info(f"Successfully retrieved content from {url}")
            soup = BeautifulSoup(response.content, 'html.parser')

            # Search for all the text containing the search term
            matches = soup.find_all(string=lambda text: search_term.lower() in text.lower())

            if matches:
                logging.info(f"Found relevant section for: {search_term}")

                # Find the link to the Excel file (look for 'href' attribute containing '.xls' or '.xlsx')
                excel_link = None
                for link in soup.find_all('a', href=True):
                    if '.xls' in link['href'] or '.xlsx' in link['href']:
                        excel_link = link['href']
                        break

                if excel_link:
                    logging.info(f"Excel file found: {excel_link}")
                    file_name = excel_link.split('/')[-1]
                    logging.info(f"File name extracted: {file_name}")

                    # If the link is relative, make it an absolute URL
                    if not excel_link.startswith('http'):
                        excel_link = 'https://www.gov.uk' + excel_link

                    # Download and check the Excel file for the latest quarter
                    should_download, latest_quarter = check_excel_for_latest_quarter(excel_link, latest_quarter, excel_save_path, file_name)

                    if should_download:
                        logging.info(f"Newer data available. Downloading the file and updating latest quarter to {latest_quarter}.")
                        download_excel_file(excel_link, excel_save_path, file_name)
                    else:
                        logging.info("The Excel file does not contain data for newer quarters. No download will be performed.")
                else:
                    logging.warning("No Excel file link found.")
            else:
                logging.warning(f"No results found for the search term: {search_term}")
        else:
            logging.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# COMMAND ----------

# put in config.json new file
config = {
  "url": "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends",
  "search_term": "Supply and use of crude oil",
  "latest_quarter": "1984 1st quarter",
  "excel_save_path": "/FileStore/tables/final_data_petroineous"
}
url = config["url"]
search_term = config["search_term"]
latest_quarter = config["latest_quarter"]
excel_save_path = config["excel_save_path"]

# Call the function to search for the term and conditionally download the file
search_for_energy_trend(url, search_term, latest_quarter, excel_save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, trim, monotonically_increasing_id, when, lit, concat,concat_ws, regexp_replace,split,sum as spark_sum,substring
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# Get the list of files in the directory
logging.info("Fetching list of files from DBFS directory: /FileStore/tables/final_data_petroineous")
file_list = dbutils.fs.ls("/FileStore/tables/final_data_petroineous")

# Find the most recently modified file
file_name = max(file_list, key=lambda f: f.modificationTime).name
logging.info(f"Most recently modified file found: {file_name}")

# Construct the path to the file in DBFS
dbfs_excel_path = f"/FileStore/tables/final_data_petroineous/{file_name}"
logging.info(f"DBFS file path constructed: {dbfs_excel_path}")

# Define a temporary local path to copy the file
local_excel_path = f"/tmp/{file_name}"

# Copy the file from DBFS to the local /tmp/ directory
logging.info(f"Copying file from DBFS to local path: {local_excel_path}")
dbutils.fs.cp(f"dbfs:{dbfs_excel_path}", local_excel_path)

# Now read the Excel file using pandas from the local path
logging.info(f"Reading Excel file: {local_excel_path}")
df_quarter = pd.read_excel(local_excel_path, sheet_name='Quarter', skiprows=4)

# Convert pandas DataFrame to Spark DataFrame
logging.info("Converting pandas DataFrame to Spark DataFrame")
spark_df = spark.createDataFrame(df_quarter).withColumn("id", monotonically_increasing_id())

# Replace spaces in column names with underscores
logging.info("Replacing spaces in column names with underscores")
spark_df = spark_df.select([col(c).alias(c.replace(' ', '_')) for c in spark_df.columns])

# Rename 'Column1' to 'Category' if it exists
if 'Column1' in spark_df.columns:
    logging.info("Renaming 'Column1' to 'Category'")
    spark_df = spark_df.withColumnRenamed('Column1', 'Category')


# COMMAND ----------

# Clean the 'Category' column and remove '[note #]' patterns
logging.info("Cleaning 'Category' column: Trimming spaces and removing note patterns.")
spark_df = spark_df.withColumn("Category", trim(col("Category")))\
                   .withColumn("Category", regexp_replace(col("Category"), r"\s*\[note\s*\d+\]", ""))

# Create 'Sub_Category' based on conditions
logging.info("Creating 'Sub_Category' based on 'id' values.")
spark_df = spark_df.withColumn("Sub_Category", 
                               when(col("id").isin([1, 2, 3]), concat_ws('_', lit("Indigenous production"), col("Category")))
                               .when(col("id").isin([5, 6]), concat_ws('_', lit("Imports"), col("Category")))
                               .when(col("id").isin([8, 9]), concat_ws('_', lit("Exports"), col("Category")))
                               .otherwise(col("Category")))

# Update 'Category' if 'Sub_Category' differs
logging.info("Updating 'Category' based on 'Sub_Category' differences.")
spark_df = spark_df.withColumn("Category", when(col('Sub_Category') != col('Category'), split(col('Sub_Category'), '_')[0])
                               .otherwise(col('Category')))

# Filter for 'Indigenous production'
logging.info("Filtering DataFrame for 'Indigenous production' rows.")
indigenous_df = spark_df.filter(
    (col("Category") == "Indigenous production") & 
    (col("Sub_Category") != "Indigenous production") & 
    (col("Sub_Category") != "Indigenous production_Feedstocks")
)

# Summing relevant columns for 'Indigenous production'
logging.info("Summing columns for 'Indigenous production' group.")
columns_to_sum = [c for c in indigenous_df.columns if c not in ['Category', 'Sub_Category']]
sum_exprs = [spark_sum(col(c)).alias(c) for c in columns_to_sum]
summed_df = indigenous_df.groupBy("Category").agg(*sum_exprs).withColumn("Sub_Category", lit("Indigenous production_Crude Oil & NGLs"))

# Union the summed DataFrame with the original DataFrame
logging.info("Unioning the summed DataFrame with the original DataFrame and applying final filters.")
final_df = spark_df.unionByName(summed_df)\
                   .filter((col("Sub_Category") != "Indigenous production_Crude oil") & 
                           (col("Sub_Category") != "Indigenous production_NGLs"))

# Log the  DataFrame's  count
logging.info(f"Final DataFrame row count: {final_df.count()}")

# COMMAND ----------

# Define a function to clean and rename columns into YYYYQQ format
def rename_columns(df):
    logging.info("Starting column renaming process.")
    
    renamed_cols = []
    for col_name in df.columns:
        # Step 1: Replace newline characters and underscores with spaces, and remove extra spaces
        col_name_cleaned = col_name.replace('_\n', ' ').replace('\n', ' ').replace('_', ' ').strip()
        logging.debug(f"Cleaned column name: {col_name_cleaned}")

        # Step 2: Split column by spaces
        parts = col_name_cleaned.split(' ')
        
        # Check if the column name has at least two parts (i.e., year and quarter)
        if len(parts) >= 2:
            year = parts[0].strip()  # Extract year and remove spaces
            quarter = parts[1].strip().lower()  # Extract and clean the quarter part
            
            # Step 3: Map quarter names to corresponding numbers
            if '1st' in quarter:
                quarter_num = '01'
            elif '2nd' in quarter:
                quarter_num = '02'
            elif '3rd' in quarter or '3nd' in quarter:
                quarter_num = '03'
            elif '4th' in quarter:
                quarter_num = '04'
            else:
                quarter_num = None  # If the quarter part does not match any known format

            # Step 4: Combine year and quarter into the YYYYQQ format if both are valid
            if year.isdigit() and quarter_num:
                new_col_name = f"{year}{quarter_num}"
                renamed_cols.append((col_name, new_col_name))
                # logging.info(f"Renaming column: {col_name} -> {new_col_name}")
            else:
                renamed_cols.append((col_name, col_name))  # Keep the original name if format doesn't match
                logging.warning(f"Skipping renaming for column: {col_name} (Invalid format)")
        else:
            renamed_cols.append((col_name, col_name))  # For columns not matching the expected format
            logging.warning(f"Skipping renaming for column: {col_name} (Not enough parts)")
    
    # Step 5: Rename columns in the DataFrame
    for old_name, new_name in renamed_cols:
        df = df.withColumnRenamed(old_name, new_name)
    
    logging.info("Finished column renaming process.")
    return df

# COMMAND ----------

# Apply the rename function
logging.info("Applying the rename_columns function.")
df_renamed = rename_columns(final_df)

# List of quarterly columns (assuming they are all numeric, e.g., '199901', '199902')
logging.info("Extracting quarterly columns.")
quarter_columns = [col for col in df_renamed.columns if col.isdigit()]

# Create an expression to correctly stack the quarterly columns
logging.info("Creating expression to stack quarterly columns.")
stack_expr = ", ".join([f"'{c}', `{c}`" for c in quarter_columns])

# Unpivot the DataFrame from wide to long format
logging.info("Unpivoting the DataFrame from wide to long format.")
df_unpivot = df_renamed.select("Category", "Sub_Category", expr(f"stack({len(quarter_columns)}, {stack_expr}) as (Quarter, Quantity)"))

# Add FileName and ProcessedDate columns
logging.info("Adding FileName and ProcessedDate columns to the final DataFrame.")
finalDF = df_unpivot.withColumn("FileName", lit(file_name))\
                    .withColumn("ProcessedDate", lit(datetime.now().strftime('%d/%m/%Y')))

# Display final DataFrame information
logging.info(f"Final DataFrame row count: {finalDF.count()}")


# COMMAND ----------

# Specify the exact CSV file path and this can be configured in file
csv_save_path = "/FileStore/tables/final_data_petroineous/TransformedEnergyData.csv"

# Coalesce to 1 partition to ensure a single CSV file is created
logging.info("Coalescing DataFrame to 1 partition to ensure a single CSV file is created.")
spark_df.coalesce(1).write.mode('overwrite').option("header", "true").csv("/tmp/transformed_energy_data")

# List files in the temporary directory
logging.info("Listing files in the temporary directory: /tmp/transformed_energy_data")
files = dbutils.fs.ls("/tmp/transformed_energy_data")

# Find the CSV file part (since Spark creates part files in the directory)
logging.info("Finding the correct CSV part file in the temporary directory.")
part_file_path = None
for f in files:
    if f.name.endswith(".csv"):
        part_file_path = f.path
        logging.info(f"Found part CSV file: {part_file_path}")
        break

# Check if the CSV part file was found
if part_file_path:
    # Move and rename the part file to the final destination
    logging.info(f"Moving the file from {part_file_path} to {csv_save_path}.")
    dbutils.fs.mv(part_file_path, csv_save_path)
    logging.info(f"DataFrame saved as CSV to: {csv_save_path}")
else:
    logging.error("No CSV part file found in the temporary directory.")

