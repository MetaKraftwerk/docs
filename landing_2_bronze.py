#!/usr/bin/env python
# coding: utf-8

# ## landing_2_bronze
# 
# 
# 

# # Salesforce Landing to Bronze
# ## <landing_2_bronze>
# #### **Author**: <BUILD_USER_NAME>
# #### **Build Timestamp**: <BUILD_TIMESTAMP>
# #### **Pattern**: <PATTERN_NAME>
# #### **Pattern Version**: <PATTERN_VERSION>
# #### **Instance**: <INSTANCE_NAME>
# #### **Release**: <RELEASE_NAME>
# 
# #### Description: This notebook implements a Salesforce Landing-to-Bronze transformation for <INSTANCE_NAME> data. It reads CSV files from the landing zone, applies an explicit schema, enriches rows with pipeline metadata and hash keys for change detection, trims empty strings, truncates the target Bronze table, and writes the result in Delta format to the Bronze layer. Key features include:
# ##### 1. Storage Configuration: Paths for ADLS Gen2 and process parameters are set up.
# ##### 2. Handles landing CSV ingestion (recursively looks in the instance landing folder) and loads all found CSVs. 
# ##### 3. Applies a predefined StructType schema. 
# ##### 4. Adds metadata columns: file_path, original_file_name, RSRC (data source), RECORD_ID (UUID), and LOAD_TYPE. 
# ##### 5. Generates stable business-key hash HASH_KEY (MD5 over business keys) and hash value HASH_VALUE (MD5 of attributes for change detection). 
# ##### 6. Truncates existing Bronze table (if present) before loading. 
# <INSTANCE_DESCRIPTION>
# #### The complete representation of the Bronze <INSTANCE_NAME> table schema: 
# <add_expression_block_here>
# | Column                        | Data_type | Precision | Scale | Nullable |
# |-------------------------------|-----------|-----------|-------|----------|
# | HASH_KEY  | string   | 32        |      | false     |
# | HASH_VALUE  | string   | 32        |      | false     |
# | API_ID  | bigint   | 19        |     | true     |
# | GMT  | bigint   | 19        |     | true     |
# | AIRPORT_ID  | bigint   | 19        |     | true     |
# | IATA_CODE  | string   | 3        |     | true     |
# | CITY_IATA_CODE  | string   | 3        |     | true     |
# | ICAO_CODE  | string   | 4        |     | true     |
# | COUNTRY_ISO2  | string   | 2        |     | true     |
# | GEONAME_ID  | bigint   | 19        |     | true     |
# | LATITUDE  | decimal   | 15        | 8    | true     |
# | LONGITUDE  | decimal   | 15        | 8    | true     |
# | AIRPORT_NAME  | string   | 100        |     | true     |
# | COUNTRY_NAME  | string   | 50        |     | true     |
# | PHONE_NUMBER  | string   | 50        |     | true     |
# | TIMEZONE  | string   | 50        |     | true     || RSRC  | string   | 200        |      | false     |
# | RECORD_ID  | string   | 36        |      | false     |
# | LOAD_TYPE  | string   | 200        |      | false     |
# | file_path  | string   | 200        |      | false     |
# | original_file_name  | string   | 200        |      | false     |
# 

# In[4]:


# Import libraries
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType, DoubleType, DecimalType, DateType, LongType
#import sys
#import os
import uuid
import pytz
from pyspark.sql import Row
#import builtins

spark = SparkSession.builder.getOrCreate()


# In[5]:


# Configurations
STORAGE_ACCOUNT_NAME = 'adls2synapseintf'  # Name of the storage account
LANDING_STORAGE_ACCOUNT_CONTAINER = 'tutorialcontainer' # Name of the container for the landing data
BRONZE_STORAGE_ACCOUNT_CONTAINER = 'tutorialcontainer' # Name of the container for the bronze layer
LANDING_BASE_PATH = 'test/landing' #  this is the path where the landing tables are located
BRONZE_BASE_PATH = 'test/bronze' # this is the path where the bronze tables are located

# PATTERN_NAME = "<PATTERN_NAME>" 
PATTERN_NAME = "BRONZE_DV_PREP" 

LAYER_NAME = "BRONZE"
DATA_SOURCE= "CSV"
# INSTANCE_NAME = "<INSTANCE_NAME>".upper()
INSTANCE_NAME = "AIRPORTS".upper()
# DELIVERY_MODE = "<DELIVERY_MODE>"   
DELIVERY_MODE = "FULL"  # FULL, DELTA 


# In[6]:


def generate_md5_hash(business_key_columns):
    """
    Generates MD5 hash for business keys with null key handling:
    - If ALL BK columns are NULL or empty, they are replaced with '-1'
    - If at least one BK column has a value, NULL values remain unchanged
    
    Args:
        business_key_columns (str): List of business key column names

    Returns:
        Column: A pyspark column which calculates the md5 hash key
    """
    from pyspark.sql import functions as F
    
    # 1. Helper function to check if a value is NULL or empty
    def is_empty(c):
        return (F.col(c).isNull() | (F.trim(F.col(c)) == ""))
    
    # 2. Check if ALL BK columns are NULL/empty
    all_null_condition = None
    for col in business_key_columns:
        if all_null_condition is None:
            all_null_condition = is_empty(col)
        else:
            all_null_condition = all_null_condition & is_empty(col)
    
    # 3. For each BK column: If all are NULL -> '-1', otherwise original value (even if NULL)
    processed_columns = []
    for col in business_key_columns:
        processed_col = F.when(
            all_null_condition,
            F.lit("-1")
        ).otherwise(
            F.col(col)
        )
        processed_columns.append(processed_col.cast("string"))
    
    # 4. Concatenate the columns with '#' as separator
    concatenated = F.concat_ws("#", *processed_columns)
    
    # 5. Calculate MD5 hash
    return F.md5(concatenated)


def generate_hdiff(attributes):
    """
    Calculates the HDIFF hash value for change detection

    Args:
        attributes (list): A list of a attributes which should be hashed

    Returns:
        Column: A pyspark column which calculates the md5 hash value
    """
    hdiff_cols = []
    for attr in attributes:
        hdiff_cols.append(col(attr).cast("string"))
    
    return md5(concat_ws("#", *hdiff_cols))

def add_metadata_columns(df, file_path):
    """
    Add metadata columns to the DataFrame.

    Args:
        df (DataFrame): The DataFrame to which metadata will be added.
        file_path (str): The path of the file being processed.

    Returns:
        DataFrame: The DataFrame with added metadata columns.
    """
    return df.withColumn("file_path", lit(file_path)) \
             .withColumn("original_file_name", regexp_extract(input_file_name(), r"[^/]+$", 0)) \
             .withColumn("RSRC", lit(DATA_SOURCE))\
             .withColumn("RECORD_ID", expr("uuid()"))\
             .withColumn("LOAD_TYPE", lit(DELIVERY_MODE))

def save_to_delta(df, path):
    """
    Save the DataFrame to a Delta table.

    Args:
        df (DataFrame): The DataFrame to save.
        path (str): The path where the Delta table will be saved.
    """
    df.write.format("delta") \
      .option("delta.columnMapping.mode", "name") \
      .option("mergeSchema", "true") \
      .option("header", "true") \
      .mode("append") \
      .save(path)


def truncate_bronze_table(bronze_path):
    """
    Truncate the Bronze table before loading new data.

    Args:
        bronze_path (str): The path to the Bronze table.

    """
    if DeltaTable.isDeltaTable(spark, bronze_path):
        mssparkutils.fs.rm(bronze_path, recurse=True)  # Delete the table
        print(f"Bronze table at {bronze_path} has been truncated.")
    else:
        print(f"Bronze table does not exist at {bronze_path}. No truncation performed.")


# In[7]:


def load_csv_files(base_directory): 
    """
    Load all CSV files from the specified directory and its subfolders.

    Args:
        base_directory (str): The base directory to search for CSV files.

    Returns:
        DataFrame: The loaded DataFrame containing CSV data.
        int: The number of CSV files loaded.
    """ 
    # base_directory should be without * at the end
    base_directory = base_directory.rstrip("*")

    # Initialize an empty list for the CSV files
    csv_files = []

    # List all files in the directory
    files = mssparkutils.fs.ls(base_directory)  

    # Check CSVs directly in the base folder
    for file in files:
        if file.name.lower().endswith('.csv'):
            print(f"Found CSV in base directory: {file.path}, CSV file: {file.name}")
            csv_files.append(file.path)

    # Check subfolders (if any)
    folders = [file.path for file in files if file.isDir] 
    # List all CSV files in the filtered folders
    for folder in folders:
        print(f"Listing files in folder: {folder}")
        files_in_folder = mssparkutils.fs.ls(folder)
        for file in files_in_folder:
            if file.name.endswith('.csv'):
                print(f"Found CSV file: {file.name}")
                csv_files.append(file.path)
    
    # Load the CSV files into a DataFrame
    if csv_files:
        df = spark.read.option("header", "true") \
                .option("delimiter", ";") \
                .option("ignoreLeadingWhiteSpace", True) \
                .option("ignoreTrailingWhiteSpace", True) \
                .csv(csv_files)        
        return df, len(csv_files)
    else:
        print("No CSV files found. Please check if any files have been loaded into the Landing Zone after the Last Load Time.")
        return None, 0


# In[8]:


def process_landing_to_bronze():
    """Main function to process data from Landing to Bronze"""
    
    success_source_rows = 0
    failed_source_rows = 0
    
    try:
        # Set paths
        landing_path = f'abfss://{LANDING_STORAGE_ACCOUNT_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{LANDING_BASE_PATH}/{INSTANCE_NAME}/*'
        bronze_path = f'abfss://{BRONZE_STORAGE_ACCOUNT_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{BRONZE_BASE_PATH}/{INSTANCE_NAME}/'

        print(f"Processing {INSTANCE_NAME} from Landing to Bronze. Pipeline is starting...")

        # Load schema
        print(f"Retrieving schema for '{INSTANCE_NAME}'...")
        # Define schema directly instead of loading it from a file
        # <add_expression_block_here>
        loaded_schema = StructType([  
            StructField( "API_ID" , LongType(), True),
            StructField( "GMT" , LongType(), True),
            StructField( "AIRPORT_ID" , LongType(), True),
            StructField( "IATA_CODE" , StringType(), True),
            StructField( "CITY_IATA_CODE" , StringType(), True),
            StructField( "ICAO_CODE" , StringType(), True),
            StructField( "COUNTRY_ISO2" , StringType(), True),
            StructField( "GEONAME_ID" , LongType(), True),
            StructField( "LATITUDE" , DecimalType(15, 8), True),
            StructField( "LONGITUDE" , DecimalType(15, 8), True),
            StructField( "AIRPORT_NAME" , StringType(), True),
            StructField( "COUNTRY_NAME" , StringType(), True),
            StructField( "PHONE_NUMBER" , StringType(), True),
            StructField( "TIMEZONE" , StringType(), True)
        ])


        print(f"Schema for '{INSTANCE_NAME}' successfully loaded")
        
        # Load csv files 
        print(f"Loading {INSTANCE_NAME} csv files from {landing_path}")
        df, file_count = load_csv_files(landing_path)
        print(f"Found {file_count} csv Files for '{INSTANCE_NAME}'")

        if df is None:
            raise Exception(f"No csv files found for {INSTANCE_NAME}")

        # Apply schema
        print(f"Applying schema to the DataFrame")
        df_with_schema = spark.read.schema(loaded_schema)\
                                .option("header", "true") \
                                .option("delimiter", ";") \
                                .option("ignoreLeadingWhiteSpace", True) \
                                .option("ignoreTrailingWhiteSpace", True) \
                                .csv(df.inputFiles())
        # df_with_schema = df
        record_count = df_with_schema.count()
        print(f"{record_count} records successfully loaded with schema")

        # display(df_with_schema)

        # Add metadata columns
        print(f"Adding metadata columns to {INSTANCE_NAME} DataFrame")
        df_with_metadata = add_metadata_columns(df_with_schema, landing_path)

        
        # Calculate hash key and hash value
        # <add_expression_block_here>
        df_with_metadata = (df_with_metadata.withColumn("HASH_KEY", generate_md5_hash(["IATA_CODE"]))
                                            # Generate HDIFF (hash value of attributes for change detection)
                                            .withColumn("HASH_VALUE", generate_hdiff(["API_ID", "GMT", "AIRPORT_ID", "CITY_IATA_CODE", "ICAO_CODE", "COUNTRY_ISO2", "GEONAME_ID", "LATITUDE", "LONGITUDE", "AIRPORT_NAME", "COUNTRY_NAME", "PHONE_NUMBER", "TIMEZONE"]))
                            )

        # Truncate the Bronze table  
        truncate_bronze_table(bronze_path)

        # Save as Delta
        print(f"Saving {INSTANCE_NAME} DataFrame in Delta format to: {bronze_path}")
        save_to_delta(df_with_metadata, bronze_path)
        print(f"Data successfully saved in Delta format to: {bronze_path}")
        print(f"Processing of {INSTANCE_NAME} from Landing to Bronze completed successfully")
        
    except Exception as e:
        error_message = str(e)
        print(f"Error processing {INSTANCE_NAME}: {error_message}")
        raise
    finally:
        print("LANDING_2_BRONZE Pipeline completed")


# In[9]:


if __name__ == "__main__":
    # Main execution
    print(f"Starting LDG_2_BRONZE processing for {INSTANCE_NAME}")

    process_landing_to_bronze()


# In[10]:


#test 
df = spark.read.format("delta").load(f'abfss://tutorialcontainer@adls2synapseintf.dfs.core.windows.net/test/bronze/{INSTANCE_NAME}/')
display(df.limit(100))
#df.printSchema()


