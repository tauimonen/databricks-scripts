"""
====================================================================================
Script Name:    countries_join.py
Author:         tauimonen
Date:           2025-10-26
Version:        1.0
Description:    
    This Databricks PySpark script loads, cleans, and consolidates country-level 
    population data with corresponding region and sub-region information.
    The final dataset is saved as a managed Delta table for analytics and reporting.

Inputs:
    - <base_path>/countries_population/
    - <base_path>/country_regions/
    - <base_path>/country_sub_regions/

Output:
    - A managed Delta table containing consolidated country, region, and sub-region data.

Usage:
    Run in Databricks environment. 
    Update the 'base_path' variable if the root data location changes.
    Replace <ADD_YOUR_TABLE_NAME_HERE> with the target table name before execution.

Notes:
    - All joins are left joins to preserve country records even if regional data is missing.
    - Table is created using Databricks Delta format (default for saveAsTable()).
====================================================================================
"""

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# Read the CSV files and select and rename the relevant columns
df_countries = spark.read.format('csv').option('header', 'true').\
    load("/Volumes/population/landing/datasets/countries_dataset/csv_data/countries_population/").\
    select("country_id", col("name").alias("country"), "population", "area_km2", "region_id", "sub_region_id")
        
df_regions = spark.read.csv("/Volumes/population/landing/datasets/countries_dataset/csv_data/country_regions/", header=True).\
    select(col("ID").alias("id"), col("NAME").alias("region"))

df_sub_regions = spark.read.csv("/Volumes/population/landing/datasets/countries_dataset/csv_data/country_sub_regions/", header=True).\
    select(col("ID").alias("id"), col("NAME").alias("sub_region"))

# Join countries with regions and subregions
df_final = df_countries.join(df_regions, df_countries.region_id == df_regions.id, "left").\
    select("country_id", "country", "region", "population", "area_km2", "sub_region_id").\
    join(df_sub_regions, df_countries.sub_region_id == df_sub_regions.id, "left").\
    select("country_id","country","region", "sub_region","population", "area_km2")
    
# Save the final DataFrame as a delta table
df_final.write.saveAsTable(<ADD_YOUR_TABLE_NAME_HERE>)