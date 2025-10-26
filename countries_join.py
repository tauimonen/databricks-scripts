from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

df_countries = spark.read.format('csv').option('header', 'true').\
    load("/Volumes/population/landing/datasets/countries_dataset/csv_data/countries_population/").\
        select("country_id", col("name").alias("country"), "population", "area_km2", "region_id", "sub_region_id")
        
df_regions = spark.read.csv("/Volumes/population/landing/datasets/countries_dataset/csv_data/country_regions/", header=True).\
    select(col("ID").alias("id"), col("NAME").alias("region"))
    
df_sub_regions = spark.read.csv("/Volumes/population/landing/datasets/countries_dataset/csv_data/country_sub_regions/", header=True).\
    select(col("ID").alias("id"), col("NAME").alias("sub_region"))

df_final = df_countries.join(df_regions, df_countries.region_id == df_regions.id, "left").\
    select("country_id", "country", "region", "population", "area_km2", "sub_region_id").\
    join(df_sub_regions, df_countries.sub_region_id == df_sub_regions.id, "left").\
    select("country_id","country","region", "sub_region","population", "area_km2")
    
df_final.write.saveAsTable(<ADD_YOUR_TABLE_NAME_HERE>)