# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation: CO2 vs. Temperature
# MAGIC To recap, we'll be studying the correlation between CO2 Emissions and Temperatures around the world. 
# MAGIC 
# MAGIC Some of the questions we want to answer:
# MAGIC * Which countries are worse-hit (higher temperature anomalies)?
# MAGIC * Which countries are the biggest emitters? 
# MAGIC * What are some attempts of ranking “biggest polluters” in a sensible way?
# MAGIC 
# MAGIC Since the point of this exercise is to learn how to work with data and the datasets from OWID and Kaggle are both too clean and curated, a set of dirtied data is provided.
# MAGIC 
# MAGIC They can be found at:
# MAGIC * https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/EmissionsByCountry.csv
# MAGIC * https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/GlobalTemperatures.csv
# MAGIC * https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/TemperaturesByCountry.csv
# MAGIC 
# MAGIC In the [last exercise](https://github.com/data-derp/exercise-co2-vs-temperature#data-ingestion), we ingested those CSVs, performed some light transformation in the columns, and wrote them out to Parquet files.
# MAGIC 
# MAGIC These parquet files still require some transformation in order to bring us value and that's what we'll do in this "Data Transformation" exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestones
# MAGIC 1. Join together Global Temperature and Emission Data on an Annual basis
# MAGIC 2. Join together Per Country Temperature and Emission Data on an Annual basis
# MAGIC 3. Get the Emissions of the Big Three of Europe
# MAGIC 4. Oceania Emissions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Milestone 1: Global Temperature and CO2 Emission Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Download data from an online repository into a Spark DataFrame  
# MAGIC 
# MAGIC Ideally, you could already resume where you left off in the last exercise (i.e. picking up the Parquet files you generated yourself)  
# MAGIC However, to ensure reproducibility for all participants, you can also follow the first exercise below to download the correct Parquet files
# MAGIC 
# MAGIC - Reminder: Spark (at the time of writing) does not support reading data directly from arbitrary http(s) addresses.
# MAGIC - In practice, **you would not want to do this with big data anyways**.  
# MAGIC   It's much better to load big data into a data lake (e.g. S3, Azure Data Lake Storage Gen2, or Azure Blob Storage).  
# MAGIC   You can then use the connectors that come with your cloud offering (e.g. Databricks, AWS Glue, or AWS EMR) to read from these data lakes.  
# MAGIC   [See Example](https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started.html#get-started-with-azure-data-lake-storage-gen2)
# MAGIC   
# MAGIC - For the sake of simplicity in this exercise, let's not worry about transferring these datasets to S3 yet
# MAGIC 
# MAGIC **Objectives**
# MAGIC - Let's simply download the data first, then read the Parquet files with Spark
# MAGIC - Run the following cells and inspect the resulting DataFrames

# COMMAND ----------

# MAGIC %pip install wget

# COMMAND ----------

# Set URLS of input parquet data

CO2_URL = "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-transformation/input-data/EmissionsByCountry.parquet/part-00000-a5120099-3f2e-437a-98c6-feb2845cdf28-c000.snappy.parquet"

GLOBAL_TEMPERATURES_URL = "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-transformation/input-data/GlobalTemperatures.parquet/part-00000-f77d0e73-78da-48a2-be74-681dd35a82cf-c000.snappy.parquet"

TEMPERATURES_BY_COUNTRY_URL = "https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-transformation/input-data/TemperaturesByCountry.parquet/part-00000-b9e4293b-b7a5-4582-86d1-eccf44649b40-c000.snappy.parquet"

# COMMAND ----------

'''
Clear out existing working directory
'''
current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/dataTransformation"
dbutils.fs.rm(working_directory, True)

# COMMAND ----------

# Download files to the local filesystem
import os
import wget
import sys
import shutil

 
sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

LOCAL_DIR = f"{os.getcwd()}/{current_user}/dataTransformation"

shutil.rmtree(LOCAL_DIR)
os.makedirs(LOCAL_DIR)
URLS = [CO2_URL, GLOBAL_TEMPERATURES_URL, TEMPERATURES_BY_COUNTRY_URL]
filenames = []

for url in URLS:
  filename = url.split("/")[-2]
  filenames.append(filename)
  saved_filename = wget.download(url, out = f"{LOCAL_DIR}/{filename}")
  print(f"Saved in: {saved_filename}")

# COMMAND ----------

# Set up a DBFS directory to keep a copy of our data in a distributed filesystem (rather than local/single-node)
# Copy over the files from the local filesystem to our new DBFS directory
# (so that Spark can read in a performant manner from dbfs:/)

for filename in filenames: # copy from local file system into a distributed file system such as DBFS
  dbutils.fs.cp(f"file:{LOCAL_DIR}/{filename}", f"""{working_directory}/{filename}""")
  
DBFS_FILEPATHS = [x.path for x in dbutils.fs.ls(working_directory)]
print(DBFS_FILEPATHS)
CO2_PATH, GLOBAL_TEMPERATURES_PATH, TEMPERATURES_BY_COUNTRY_PATH = DBFS_FILEPATHS

# COMMAND ----------

# Load EmissionsByCountry Parquet
co2_df = spark.read.parquet(CO2_PATH)

# COMMAND ----------

display(co2_df)

# COMMAND ----------

# Load GlobalTemperatures Parquet
global_temperatures_df = spark.read.parquet(GLOBAL_TEMPERATURES_PATH)
display(global_temperatures_df)

# COMMAND ----------

# Load TemperaturesByCountry Parquet
temperatures_by_country_df = spark.read.parquet(TEMPERATURES_BY_COUNTRY_PATH)
display(temperatures_by_country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Get Country Emissions
# MAGIC **Topics:** filter, cast, select, alias
# MAGIC 
# MAGIC Read EmissionsByCountry.parquet into a Spark DataFrame. Make sure that each Entity is a country.
# MAGIC 
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - Country: string
# MAGIC     - TotalEmissions: float
# MAGIC     - PerCapitaEmissions: float
# MAGIC     - ShareOfGlobalEmissions: float

# COMMAND ----------

from typing import Dict
from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F
from pyspark.sql.types import *

def get_country_emissions(co2_df: DataFrame) -> DataFrame:
  # You'll notice that there's an Entity called "World".
  # Since we're analyzing emissions of countries, let's discard "World"
  discard_element: String = NotImplemented #TODO
  if discard_element is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO TV")

  country_emissions = co2_df \
      .filter(F.col("Entity") != F.lit(discard_element)) \
      .select(
          F.col("Year"),
          F.col("Entity").alias("Country"),
          F.col("Annual_CO2_emissions").cast(FloatType()).alias("TotalEmissions"),
          F.col("Per_capita_CO2_emissions").cast(FloatType()).alias("PerCapitaEmissions"),
          F.col("Share_of_global_CO2_emissions").cast(FloatType()).alias("ShareOfGlobalEmissions"),
      )
  return country_emissions

# COMMAND ----------

####### SOLUTION #######

from typing import Dict
from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F
from pyspark.sql.types import *

def get_country_emissions(co2_df: DataFrame) -> DataFrame:
  # You'll notice that there's an Entity called "World".
  # Since we're analyzing emissions of countries, let's discard "World"
  discard_element: String = "World" #TODO
  if discard_element is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO TV")

  country_emissions = co2_df \
      .filter(F.col("Entity") != F.lit(discard_element)) \
      .select(
          F.col("Year"),
          F.col("Entity").alias("Country"),
          F.col("Annual_CO2_emissions").cast(FloatType()).alias("TotalEmissions"),
          F.col("Per_capita_CO2_emissions").cast(FloatType()).alias("PerCapitaEmissions"),
          F.col("Share_of_global_CO2_emissions").cast(FloatType()).alias("ShareOfGlobalEmissions"),
      )
  return country_emissions

# COMMAND ----------

import pandas as pd
import numpy as np
from typing import List, Union

def prepare_frame(
    df: pd.DataFrame, column_order: List[str] = None, sort_keys: List[str] = None,
    ascending: Union[bool, List[bool]] = True, reset_index: bool = True):
    """Prepare Pandas DataFrame for equality check"""
    if column_order is not None: df = df.loc[:, column_order]
    if sort_keys is not None: df = df.sort_values(sort_keys, ascending=ascending)
    if reset_index: df = df.reset_index(drop=True)
    return df

def test_get_country_emissions():
  """Tests the get_country_emissions method"""
  input_pandas = pd.DataFrame({
      "Year": [1999, 2000, 2001, 2020, 2021],
      "Entity" : ["World", "World", "World", "Fiji", "Argentina"],
      "Annual_CO2_emissions": [1.0, 2.0, 3.0, 4.0, 5.0],
      "Per_capita_CO2_emissions": [1.0, 2.0, 3.0, 4.0, 5.0],
      "Share_of_global_CO2_emissions": [0.5, 0.5, 0.5, 0.5, 0.5]
  })
  input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Entity", StringType(), True),
      StructField("Annual_CO2_emissions", FloatType(), True),
      StructField("Per_capita_CO2_emissions", FloatType(), True),
      StructField("Share_of_global_CO2_emissions", FloatType(), True),
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "Country", "TotalEmissions", "PerCapitaEmissions", "ShareOfGlobalEmissions"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([2020, 2021], dtype=np.dtype("int32")),
      "Country" : pd.Series(["Fiji", "Argentina"], dtype=str),
      "TotalEmissions": pd.Series([4.0, 5.0], dtype=np.dtype("float32")),
      "PerCapitaEmissions": pd.Series([4.0, 5.0], dtype=np.dtype("float32")),
      "ShareOfGlobalEmissions": pd.Series([0.5, 0.5], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  output_df = get_country_emissions(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert "World" not in output_pandas["Country"].str.title().values
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
        
test_get_country_emissions()

# COMMAND ----------

display(get_country_emissions(co2_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Aggregate Country Emissions
# MAGIC **Topics:** aggregate functions, alias
# MAGIC 
# MAGIC Aggregate the total CO2 emissions globally on an ANNUAL basis.
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC - Year: integer
# MAGIC - TotalEmissions: float

# COMMAND ----------

def aggregate_global_emissions(country_emissions: DataFrame) -> DataFrame:
  # TODO: Exercise
  groupByElement: String = NotImplemented
  
  if groupByElement is NotImplemented:
    raise NotImplemented("DO YOUR HOMEWORK OR NO SPOTIFY") 
  global_emissions = country_emissions.groupBy(groupByElement).agg(
      F.sum(F.col("TotalEmissions")).cast(FloatType()).alias("TotalEmissions")
  )
  return global_emissions

# COMMAND ----------

####### SOLUTION #######
def aggregate_global_emissions(country_emissions: DataFrame) -> DataFrame:
  # TODO: Exercise
  groupByElement: String = "Year"
  
  if groupByElement is NotImplemented:
    raise NotImplemented("DO YOUR HOMEWORK OR NO SPOTIFY") 
  global_emissions = country_emissions.groupBy(groupByElement).agg(
      F.sum(F.col("TotalEmissions")).cast(FloatType()).alias("TotalEmissions")
  )
  return global_emissions

# COMMAND ----------

def test_aggregate_global_emissions():
  """Tests the aggregate_global_emissions method"""
  input_pandas = pd.DataFrame({
      "Year": [1999, 1999, 1999, 2020, 2020, 2021],
      "Country" : ["Tuvalu", "Lichtenstein", "Togo", "Singapore", "Fiji", "Argentina"],
      "TotalEmissions": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
      "PerCapitaEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      "ShareOfGlobalEmissions": [0.33, 0.33, 0.33, 0.5, 0.5, 0.5]
  })
  input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True),
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "TotalEmissions"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020, 2021], dtype=np.dtype("int32")),
      "TotalEmissions": pd.Series([6.0, 9.0, 6.0], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  output_df = aggregate_global_emissions(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")

test_aggregate_global_emissions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Aggregate Global Temperatures
# MAGIC **Topics:** aggregate functions, alias
# MAGIC 
# MAGIC Aggregate temperature measurements globally on an ANNUAL basis.
# MAGIC Think carefully about the appropriate aggregation functions to use.
# MAGIC For this project, you can just ignore any 'Uncertainty' columns.
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - LandAverageTemperature: float
# MAGIC     - LandMaxTemperature: float
# MAGIC     - LandMinTemperature: float
# MAGIC     - LandAndOceanAverageTemperature: float

# COMMAND ----------

def aggregate_global_temperatures(temperatures_global_df: DataFrame) -> DataFrame:
  temps_global_df = temperatures_global_df.withColumn("Year", F.year(F.col("Date")))

  global_temperatures = temps_global_df.groupBy("Year").agg(
      # TODO: Exercise
  )
  return global_temperatures


# COMMAND ----------

####### SOLUTION #######

def aggregate_global_temperatures(temperatures_global_df: DataFrame) -> DataFrame:
  temps_global_df = temperatures_global_df.withColumn("Year", F.year(F.col("Date")))

  global_temperatures = temps_global_df.groupBy("Year").agg(
    F.avg("LandAverageTemperature").cast(FloatType()).alias("LandAverageTemperature"), # TODO: Exercise
    F.max("LandMaxTemperature").cast(FloatType()).alias("LandMaxTemperature"), # TODO: Exercise
    F.min("LandMinTemperature").cast(FloatType()).alias("LandMinTemperature"), # TODO: Exercise
    F.avg("LandAndOceanAverageTemperature").cast(FloatType()).alias("LandAndOceanAverageTemperature") # TODO: Exercise
  )
  return global_temperatures


# COMMAND ----------

from datetime import datetime

def test_aggregate_global_temperatures():
  input_pandas = pd.DataFrame({
      "Date": [datetime(1999, 11, 1), datetime(1999, 12, 1), datetime(2020, 1, 1), datetime(2020, 2, 1), datetime(2020, 3, 1)],
      "LandAverageTemperature": [1.0, 0.0, 1.0, 2.0, 3.0],
      "LandMaxTemperature": [6.0, 9.0, 6.9, 4.20, 6.9420],
      "LandMinTemperature": [-6.0, -9.0, -6.9, -4.20, -6.9420],
      "LandAndOceanAverageTemperature": [1.0, 0.0, 1.0, 2.0, 3.0]
  })
  input_schema = StructType([
      StructField("Date", TimestampType(), True),
      StructField("LandAverageTemperature", FloatType(), True),
      StructField("LandMaxTemperature", FloatType(), True),
      StructField("LandMinTemperature", FloatType(), True),
      StructField("LandAndOceanAverageTemperature", FloatType(), True),
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "LandAverageTemperature", "LandMaxTemperature", "LandMinTemperature", "LandAndOceanAverageTemperature"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020], dtype=np.dtype("int32")),
      "LandAverageTemperature": pd.Series([0.5, 2.0], dtype=np.dtype("float32")),
      "LandMaxTemperature": pd.Series([9.0, 6.9420], dtype=np.dtype("float32")),
      "LandMinTemperature": pd.Series([-9.0, -6.9420], dtype=np.dtype("float32")),
      "LandAndOceanAverageTemperature": pd.Series([0.5, 2.0], dtype=np.dtype("float32")),
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  output_df = aggregate_global_temperatures(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
  
test_aggregate_global_temperatures()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Global Emissions and Temperature
# MAGIC **Topics:** joins
# MAGIC 
# MAGIC Perform an INNER JOIN between the results of
# MAGIC   1. aggregate_global_emissions
# MAGIC   2. aggregate_global_temperatures
# MAGIC 
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - TotalEmissions: float
# MAGIC     - LandAverageTemperature: float
# MAGIC     - LandMaxTemperature: float
# MAGIC     - LandMinTemperature: float
# MAGIC     - LandAndOceanAverageTemperature: floats

# COMMAND ----------

def join_global_emissions_temperatures(
  global_emissions: DataFrame, 
  global_temperatures: DataFrame
  ) -> DataFrame:

  global_emissions_temperatures = global_emissions.join(
      # TODO: Exercise
  ) 
  return global_emissions_temperatures

# COMMAND ----------

####### SOLUTION #######
def join_global_emissions_temperatures(
  global_emissions: DataFrame, 
  global_temperatures: DataFrame
  ) -> DataFrame:

  global_emissions_temperatures = global_emissions.join(
    global_temperatures, on="Year", how="inner" # TODO: Exercise
  ) 
  return global_emissions_temperatures 


# COMMAND ----------

def test_join_global_emissions_temperatures():
  emissions_input_pandas = pd.DataFrame({
      "Year": [1999, 2020, 2021],
      "TotalEmissions": [6.0, 9.0, 6.0]
  })
  emissions_input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("TotalEmissions", FloatType(), True)
  ])
  emissions_input_df = spark.createDataFrame(emissions_input_pandas, emissions_input_schema)

  temperatures_input_pandas = pd.DataFrame({
      "Year": [1999, 2020],
      "LandAverageTemperature": [0.5, 2.0],
      "LandMaxTemperature": [9.0, 6.9420],
      "LandMinTemperature": [-9.0, -6.9420],
      "LandAndOceanAverageTemperature": [0.5, 2.0],
  })
  temperatures_input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("LandAverageTemperature", FloatType(), True),
      StructField("LandMaxTemperature", FloatType(), True),
      StructField("LandMinTemperature", FloatType(), True),
      StructField("LandAndOceanAverageTemperature", FloatType(), True),
  ])
  temperatures_input_df = spark.createDataFrame(temperatures_input_pandas, temperatures_input_schema)

  expected_columns = ["Year", "TotalEmissions", "LandAverageTemperature", "LandMaxTemperature", "LandMinTemperature", "LandAndOceanAverageTemperature"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020], dtype=np.dtype("int32")),
      "TotalEmissions": pd.Series([6.0, 9.0], dtype=np.dtype("float32")),
      "LandAverageTemperature": pd.Series([0.5, 2.0], dtype=np.dtype("float32")),
      "LandMaxTemperature": pd.Series([9.0, 6.9420], dtype=np.dtype("float32")),
      "LandMinTemperature": pd.Series([-9.0, -6.9420], dtype=np.dtype("float32")),
      "LandAndOceanAverageTemperature": pd.Series([0.5, 2.0], dtype=np.dtype("float32")),
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  output_df = join_global_emissions_temperatures(emissions_input_df, temperatures_input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
  
test_join_global_emissions_temperatures()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's put it all together

# COMMAND ----------

country_emissions: DataFrame = get_country_emissions(co2_df)
global_emissions: DataFrame = aggregate_global_emissions(country_emissions)
global_temperatures: DataFrame = aggregate_global_temperatures(global_temperatures_df)
global_emissions_temperatures: DataFrame = join_global_emissions_temperatures(
    global_emissions, 
    global_temperatures
)
display(global_emissions_temperatures)


# COMMAND ----------

# Write DataFrame to Parquet
global_emissions_temperatures.coalesce(1).orderBy("Year") \
    .write.format("parquet").mode("overwrite") \
    .parquet(f"{working_directory}/GlobalEmissionsVsTemperatures.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestone 2: Per Country Temperature and CO2 Emission Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Remove Lenny Face
# MAGIC **Objective:** Convert an incoming string into a format that can be casted to a FloatType by Spark. Spark is smart enough to convert "69.420" to 69.420 but <69.420> will be casted to a null. To keep the exercise simple (no regex required), you'll only need to handle the Lenny face.
# MAGIC 
# MAGIC **HINT:** only temperature entries with Lenny's face are valid measurements.
# MAGIC There are multiple ways to tackle this: udf, pandas_udf, regexp_extract, regexp_replace, etc. Normally, we'd recommend a pandas_udf as it's a nice transferrable skill with good performance. However, to keep this job simple, let's use a standard Python function. We will later convert this function (remove_lenny_face) to a UDF in the aggregate_country_temperatures function.
# MAGIC 
# MAGIC The point is to demonstrate that you can write arbitrary Python logic as a UDF if Spark doesn't have the built-in function you need.

# COMMAND ----------

display(temperatures_by_country_df)

# COMMAND ----------

def remove_lenny_face(temperature: str) -> str:
  fixed_temperature_string = NotImplemented # TODO: Exercise
  if fixed_temperature_string is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO CHOCOLATE")
  return fixed_temperature_string

# COMMAND ----------

####### SOLUTION #######
def remove_lenny_face(temperature: str) -> str:
  fixed_temperature_string = temperature.replace("( ͡° ͜ʖ ͡°)", "") # TODO: exercise
  if fixed_temperature_string is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO CHOCOLATE")
  return fixed_temperature_string

# COMMAND ----------

def test_remove_lenny_face():
  original = pd.Series(["( ͡° ͜ʖ ͡°)4.384( ͡° ͜ʖ ͡°)", "-", "?", "#", "( ͡° ͜ʖ ͡°)1.53( ͡° ͜ʖ ͡°)"])
  try:
      result = original.map(remove_lenny_face)
      assert result.to_list() == ["4.384", "-", "?", "#", "1.53"] # make valid floats parsable
      spark_df = spark.createDataFrame(pd.DataFrame({"lol": result}))
      spark_df = spark_df.withColumn("lmao", F.col("lol").cast(FloatType())) # automatic casting with spark
      assert spark_df.filter(F.col("lmao").isNull()).count() == 3
      assert spark_df.filter(F.col("lmao").isNotNull()).count() == 2
  except Exception as e:
      raise type(e)(''.join(debug(original))) from e
  print("All tests passed :)")
  
test_remove_lenny_face()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Fix Country
# MAGIC Use built-in Spark functions to clean-up the "Country" column e.g. "   cAnAdA " -> "Canada". Don't forget about those annoying leading/trailing spaces.

# COMMAND ----------

display(temperatures_by_country_df)

# COMMAND ----------

def fix_country(col: Column) -> Column:
  fixed_country = NotImplemented # TODO: Exercise
  if fixed_country is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO TV")
  return fixed_country

# COMMAND ----------

####### SOLUTION #######
def fix_country(col: Column) -> Column:
  return F.initcap(F.lower(F.trim(col)))

# COMMAND ----------

def test_fix_country(self):
  original = pd.Series(["  gErMaNy ", "   uNiTeD sTaTeS    "])
  spark_df = self.spark.createDataFrame(pd.DataFrame({"Country": original}))
  spark_df = spark_df.withColumn("Country", self.transformer.fix_country(F.col("Country")))
  fixed = spark_df.toPandas()
  try:
    result = sorted(fixed["Country"])
    assert result == ["Germany", "United States"]
  except Exception as e:
    raise type(e)(''.join(debug(original))) from e
  print("All tests pass! :)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Aggregate Country Temperatures
# MAGIC **Topics:** casting, udf/pandas_udf, aggregation functions
# MAGIC 
# MAGIC Aggregate temperature measurements per country on an ANNUAL basis.
# MAGIC INVESTIGATE the data to look for any data quality issues
# MAGIC Think carefully about:
# MAGIC - any necessary cleaning (WARNING: don't assume Spark can intelligently read/cast everything)
# MAGIC - the appropriate aggregation function to use
# MAGIC 
# MAGIC For this project, you can just ignore any 'Uncertainty' columns.
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - Country: string
# MAGIC     - AverageTemperature: float

# COMMAND ----------

def aggregate_country_temperatures(temperatures_country_df: DataFrame) -> DataFrame:
    # Register your function as a UDF
    fix_temperature_udf = F.udf(remove_lenny_face, returnType=StringType())
    temperature_expr = fix_temperature_udf(F.col("AverageTemperature")).cast(FloatType())

    # Unlike the Global Temperatures dataset...Spark couldn't automatically parse the "Date" column as a timestamp
    # You'll have to find a built-in function to perform the conversion then extract the year
    year_expr = F.year(F.to_timestamp(F.col("Date"), format="MM-dd-yyyy")) # TODO: Exercise
    country_expr = fix_country(F.col("Country")) # TODO: Exercise

    country_temperatures = NotImplemented # TODO: Exercise
    if country_temperatures is NotImplemented:
        raise NotImplemented("DO YOUR HOMEWORK OR NO ICE CREAM")
    return country_temperatures

# COMMAND ----------

####### SOLUTION #######

def aggregate_country_temperatures(temperatures_country_df: DataFrame) -> DataFrame:
  # Register your function as a UDF
  fix_temperature_udf = F.udf(remove_lenny_face, returnType=StringType())
  temperature_expr = fix_temperature_udf(F.col("AverageTemperature")).cast(FloatType())

  # Unlike the Global Temperatures dataset...Spark couldn't automatically parse the "Date" column as a timestamp
  # You'll have to find a built-in function to perform the conversion then extract the year
  year_expr = F.year(F.to_timestamp(F.col("Date"), format="MM-dd-yyyy")) # TODO: Exercise
  country_expr = fix_country(F.col("Country")) # TODO: Exercise

  # TODO: exercise
  cleaned_df = temperatures_country_df.select(
      year_expr.alias("Year"),
      country_expr.alias("Country"),
      temperature_expr.alias("AverageTemperature")
  )
  country_temperatures = cleaned_df.groupBy("Year", "Country").agg( 
      F.avg(F.col("AverageTemperature")).cast(FloatType()).alias("AverageTemperature")
  )
  return country_temperatures

# COMMAND ----------

def test_aggregate_country_temperatures():
  input_pandas = pd.DataFrame({
      "Date": ["11-30-1999", "12-31-1999", "01-01-2020", "02-01-2020", "03-01-2020"],
      "Country":  [" bRaZiL  ", "   BrAzIl ", "japaN", "OMAN", "oman"],
      "AverageTemperature": ["( ͡° ͜ʖ ͡°)1.0( ͡° ͜ʖ ͡°)", "( ͡° ͜ʖ ͡°)0.0( ͡° ͜ʖ ͡°)", "-", "?", "( ͡° ͜ʖ ͡°)3.0( ͡° ͜ʖ ͡°)"]
  })
  input_schema = StructType([
      StructField("Date", StringType(), True),
      StructField("Country", StringType(), True),
      StructField("AverageTemperature", StringType(), True)
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "Country", "AverageTemperature"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020, 2020], dtype=np.dtype("int32")),
      "Country": pd.Series(["Brazil", "Japan", "Oman"], dtype=np.dtype("O")),
      "AverageTemperature": pd.Series([0.5, np.nan, 3.0], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  output_df = aggregate_country_temperatures(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")

test_aggregate_country_temperatures()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Country Emissions Temperatures
# MAGIC Topics: joins
# MAGIC 
# MAGIC Perform an INNER JOIN between the results of:
# MAGIC   1. get_country_emissions
# MAGIC   2. aggregate_country_temperatures
# MAGIC 
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - Country: string
# MAGIC     - TotalEmissions: float
# MAGIC     - PerCapitaEmissions: float
# MAGIC     - ShareOfGlobalEmissions: float
# MAGIC     - AverageTemperature: float
# MAGIC 
# MAGIC **HINT:** don't forget a slight modification compared to the join_global_emissions_temperatures function
# MAGIC In the real world, you should always make sure that the country names have been standardized.
# MAGIC However, for our exercise, just assume that a no-match is truly no-match.

# COMMAND ----------

def join_country_emissions_temperatures(
  country_emissions: DataFrame,
  country_temperatures: DataFrame) -> DataFrame:

  country_emissions_temperatures = country_emissions.join(
      # TODO: Exercise
  )
  return country_emissions_temperatures

# COMMAND ----------

####### SOLUTION #######
def join_country_emissions_temperatures(
  country_emissions: DataFrame,
  country_temperatures: DataFrame) -> DataFrame:

  country_emissions_temperatures = country_emissions.join(country_temperatures, on=["Year", "Country"], how="inner") # TODO: Exercise
  return country_emissions_temperatures

# COMMAND ----------

def test_join_country_emissions_temperatures():
  emissions_input_pandas = pd.DataFrame({
      "Year": [1999, 2000, 2001, 2020, 2021],
      "Country" : ["Brazil", "Tunisia", "Russia", "Oman", "Indonesia"],
      "TotalEmissions": [1.0, 2.0, 3.0, 4.0, 5.0],
      "PerCapitaEmissions": [0.1, 0.2, 0.3, 0.4, 0.5],
      "ShareOfGlobalEmissions": [0.1, 0.2, 0.3, 0.4, 0.5]
  })
  emissions_input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True)
  ])
  emissions_input_df = spark.createDataFrame(emissions_input_pandas, emissions_input_schema)

  temperatures_input_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020, 2020], dtype=np.dtype("int32")),
      "Country": pd.Series(["Brazil", "Japan", "Oman"], dtype=np.dtype("O")),
      "AverageTemperature": pd.Series([0.5, np.nan, 3.0], dtype=np.dtype("float32"))
  })
  temperatures_input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("AverageTemperature", FloatType(), True)
  ])
  temperatures_input_df = spark.createDataFrame(temperatures_input_pandas, temperatures_input_schema)

  expected_columns = ["Year", "Country", "TotalEmissions", "PerCapitaEmissions", "ShareOfGlobalEmissions", "AverageTemperature"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020], dtype=np.dtype("int32")),
      "Country": pd.Series(["Brazil", "Oman"], dtype=np.dtype("O")),
      "TotalEmissions": pd.Series([1.0, 4.0], dtype=np.dtype("float32")),
      "PerCapitaEmissions": pd.Series([0.1, 0.4], dtype=np.dtype("float32")),
      "ShareOfGlobalEmissions": pd.Series([0.1, 0.4], dtype=np.dtype("float32")),
      "AverageTemperature": pd.Series([0.5, 3.0], dtype=np.dtype("float32")),
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  output_df = join_country_emissions_temperatures(emissions_input_df, temperatures_input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
  
test_join_country_emissions_temperatures()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's put it together

# COMMAND ----------

country_temperatures: DataFrame = aggregate_country_temperatures(temperatures_by_country_df)
country_emissions_temperatures: DataFrame = join_country_emissions_temperatures(
    country_emissions, 
    country_temperatures
)
display(country_emissions_temperatures)

# COMMAND ----------

country_emissions_temperatures.coalesce(1).orderBy("Year") \
    .write.format("parquet").mode("overwrite") \
    .save(f"{working_directory}/CountryEmissionsVsTemperatures.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestone 3: Get the Emissions of the Big Three of Europe

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Return Emissions for France, Germany, and UK
# MAGIC **Topics:** filter, pivot (with distinct values hinting) 
# MAGIC 
# MAGIC Using the result of get_country_emissions, filter for 1900 onwards only.
# MAGIC Next, reshape the data to satisfy the requirements below.
# MAGIC 
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - France_TotalEmissions: float
# MAGIC     - France_PerCapitaEmissions: float
# MAGIC     - Germany_TotalEmissions: float
# MAGIC     - Germany_PerCapitaEmissions: float
# MAGIC     - UnitedKingdom_TotalEmissions: float
# MAGIC     - UnitedKingdom_PerCapitaEmissions: float

# COMMAND ----------

def reshape_europe_big_three_emissions(country_emissions: DataFrame) -> DataFrame:
  modern_era_df = country_emissions.filter(F.col("Year") >= F.lit(1900))
  # TODO: exercise
  europe_big_three_emissions = NotImplemented
  if europe_big_three_emissions is NotImplemented:
      raise NotImplemented("DO YOUR HOMEWORK OR NO PIZZA")

  # You might've noticed that "United Kingdom" has a space. 
  # If you recall, spaces are not permitted in Apache Parquet column names. Let's address that:
  friendly_columns = [F.col(x).alias(x.replace(" ", "")) for x in europe_big_three_emissions.columns]
  europe_big_three_emissions = europe_big_three_emissions.select(friendly_columns)
  return europe_big_three_emissions

# COMMAND ----------

####### SOLUTION #######
def reshape_europe_big_three_emissions(country_emissions: DataFrame) -> DataFrame:
  modern_era_df = country_emissions.filter(F.col("Year") >= F.lit(1900)) 
  # TODO: exercise

  europe_big_three_emissions = modern_era_df \
    .groupBy("Year") \
    .pivot("Country", values=["France", "Germany", "United Kingdom"]) \
    .agg(
        F.first("TotalEmissions").alias("TotalEmissions"),
        F.first("PerCapitaEmissions").alias("PerCapitaEmissions")
        )
  # You might've noticed that "United Kingdom" has a space. 
  # If you recall, spaces are not permitted in Apache Parquet column names. Let's address that:
  friendly_columns = [F.col(x).alias(x.replace(" ", "")) for x in europe_big_three_emissions.columns]
  europe_big_three_emissions = europe_big_three_emissions.select(friendly_columns)
  return europe_big_three_emissions

# COMMAND ----------

def test_reshape_europe_big_three_emissions():
  input_pandas = pd.DataFrame({
              "Year": [1999, 1999, 1999, 2020, 2020, 2021, 2100],
              "Country" : ["France", "Germany", "United Kingdom", "France", "Germany", "United Kingdom", "India"],
              "TotalEmissions": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
              "PerCapitaEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
              "ShareOfGlobalEmissions": [0.33, 0.33, 0.33, 0.5, 0.5, 0.5, 1.0]
          })
  input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True),
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = [
      "Year",
      "France_TotalEmissions", "France_PerCapitaEmissions",
      "Germany_TotalEmissions", "Germany_PerCapitaEmissions",
      "UnitedKingdom_TotalEmissions", "UnitedKingdom_PerCapitaEmissions",
  ]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1999, 2020, 2021, 2100], dtype=np.dtype("int32")),
      "France_TotalEmissions": pd.Series([1.0, 4.0, np.nan, np.nan], dtype=np.dtype("float32")),
      "France_PerCapitaEmissions": pd.Series([0.1, 0.4, np.nan, np.nan], dtype=np.dtype("float32")),
      "Germany_TotalEmissions": pd.Series([2.0, 5.0, np.nan, np.nan], dtype=np.dtype("float32")),
      "Germany_PerCapitaEmissions": pd.Series([0.2, 0.5, np.nan, np.nan], dtype=np.dtype("float32")),
      "UnitedKingdom_TotalEmissions": pd.Series([3.0, np.nan, 6.0, np.nan], dtype=np.dtype("float32")),
      "UnitedKingdom_PerCapitaEmissions": pd.Series([0.3, np.nan, 0.6, np.nan], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  output_df = reshape_europe_big_three_emissions(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
  
test_reshape_europe_big_three_emissions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's put it together

# COMMAND ----------

europe_big_three_emissions: DataFrame = reshape_europe_big_three_emissions(country_emissions)
display(europe_big_three_emissions)

# COMMAND ----------

europe_big_three_emissions.coalesce(1).orderBy("Year") \
  .write.format("parquet").mode("overwrite") \
  .save(f"{working_directory}/EuropeBigThreeEmissions.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestone 4: Oceania Emissions

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Boss Battle
# MAGIC **Topics:** when (switch statements), udf/pandas_udf, Window functions, coalesce (filling nulls with a priority order)
# MAGIC 
# MAGIC The CO2 data provider for Australia and New Zealand informs you that there's a massive bug in their TotalEmissions estimations for LEAP YEARS only.
# MAGIC As a result, your team will have to produce an edited dataset for Australia and New Zealand only.
# MAGIC Using the result of get_country_emissions, disregard the TotalEmissions estimates for leap years, then replace them using the following PRIORITY:
# MAGIC 1. nearest non-null value from the past 3 years (i.e. 'forward fill')
# MAGIC 2. nearest non-null value from the future 3 years (i.e. 'backward fill')
# MAGIC 3. nullify the value (i.e. DO NOT accept the original TotalEmissions value for any leap year under any circumstance)
# MAGIC 
# MAGIC Recap:
# MAGIC - DISCARD all rows for countries other than Australia or New Zealand
# MAGIC - KEEP rows from all years (including non-leap years) for Australia or New Zealand
# MAGIC - KEEP only the following columns: Year, Country, and TotalEmissions
# MAGIC * DEFINITION of past 3 years = [2017, 2018, 2019] if the year is 2020
# MAGIC * DEFINITION of future 3 years = [2021, 2022, 2023] if the year is 2020
# MAGIC 
# MAGIC Your output Spark DataFrame's schema should be:
# MAGIC     - Year: integer
# MAGIC     - Country: string
# MAGIC     - TotalEmissions: float

# COMMAND ----------

def boss_battle(country_emissions: DataFrame) -> DataFrame:

  oceania_emissions = country_emissions.filter(F.col("Country").isin(["Australia", "New Zealand"]))

  # HINT: Python UDFs allow you to import external libraries
  def check_leap(year: int) -> bool:
      leap_bool = NotImplemented # TODO: Exercise
      if leap_bool is NotImplemented:
          raise NotImplemented("DO YOUR HOMEWORK OR NO CAKE")
      return leap_bool

  leap_year_udf = F.udf(check_leap, returnType=BooleanType())
  is_leap_year = leap_year_udf(F.col("Year"))

  # HINT: Carefully look up the Spark Window semantics
  # (partitionBy, orderBy, rowsBetween, rangeBetween)
  # Look carefully for the right Window functions to apply as well.
  w_past = NotImplemented # TODO: should be a Window (from pyspark.sql.window import Window)
  w_future = NotImplemented # TODO: should be a Window (from pyspark.sql.window import Window)
  nearest_before = NotImplemented # TODO: should be a Column Expression
  nearest_after = NotImplemented # TODO: should be a Column Expression

  if any(x is NotImplemented for x in [w_past, w_future, nearest_before, nearest_after]):
      raise NotImplemented("DO YOUR HOMEWORK OR NO CHIPS")

  # HINT: how do you choose the first column that is non-null in Spark (or SQL)? 
  emissions_prioritized = NotImplemented # TODO: should be a Column Expression (please read the HINT above)
  # HINT: how do you do perform case-switch statements in Spark?
  emissions_case = NotImplemented # TODO: should be a Column Expression (please read the HINT above)
  if any(x is NotImplemented for x in [emissions_prioritized, emissions_case]):
      raise NotImplemented("DO YOUR HOMEWORK OR NO NACHOS")

  emissions_expr = emissions_case.cast(FloatType())
  oceania_emissions_edited = oceania_emissions.select(
      "Year",
      "Country",
      emissions_expr.alias("TotalEmissions")
  )
  return oceania_emissions_edited

# COMMAND ----------

####### SOLUTION #######
from pyspark.sql.window import Window

def boss_battle(country_emissions: DataFrame) -> DataFrame:
  oceania_emissions = country_emissions.filter(F.col("Country").isin(["Australia", "New Zealand"]))

  # HINT: Python UDFs allow you to import external libraries
  from calendar import isleap
  def check_leap(year: int) -> bool:
      return isleap(year)

  leap_year_udf = F.udf(check_leap, returnType=BooleanType())
  is_leap_year = leap_year_udf(F.col("Year"))

  # HINT: Carefully look up the Spark Window semantics
  # (partitionBy, orderBy, rowsBetween, rangeBetween)
  # Look carefully for the right Window functions to apply as well.
  # TODO: Exercise
  w_past = Window().partitionBy("Country").orderBy(F.col("Year")).rangeBetween(-3, -1)
  w_future = Window().partitionBy("Country").orderBy(F.col("Year")).rangeBetween(1, 3)
  # TODO: Exercise
  nearest_before = F.last(F.col("TotalEmissions"), ignorenulls=True).over(w_past)
  nearest_after = F.first(F.col("TotalEmissions"), ignorenulls=True).over(w_future)

  if any(x is NotImplemented for x in [w_past, w_future, nearest_before, nearest_after]):
      raise NotImplemented("DO YOUR HOMEWORK OR NO CHIPS")

  # HINT: how do you choose the first column that is non-null in Spark (or SQL)? 
  emissions_prioritized = F.coalesce(
      nearest_before, # TODO: Exercise
      nearest_after, # TODO: Exercise
      F.lit(None)
  )
  # HINT: how do you do perform case-switch statements in Spark?
  emissions_case = F.when(is_leap_year, emissions_prioritized).otherwise(F.col("TotalEmissions"))
  if any(x is NotImplemented for x in [emissions_prioritized, emissions_case]):
      raise NotImplemented("DO YOUR HOMEWORK OR NO NACHOS")

  emissions_expr = emissions_case.cast(FloatType())
  oceania_emissions_edited = oceania_emissions.select(
      "Year",
      "Country",
      emissions_expr.alias("TotalEmissions")
  )
  return oceania_emissions_edited

# COMMAND ----------

def test_boss_battle():
  input_pandas = pd.DataFrame({
      "Year": [1997, 1998, 2000, 2001, 2016, 2020, 2022, 2023, 2024],
      "Country" : [
          "United States",
          "Australia", "Australia", "Australia",
          "New Zealand", "New Zealand", "New Zealand", "New Zealand", "New Zealand"],
      "TotalEmissions": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 6.5, 7.0, 8.0],
      "PerCapitaEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.8],
      "ShareOfGlobalEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.8]
  })
  input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True)
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "Country", "TotalEmissions"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([1998, 2000, 2001, 2016, 2020, 2022, 2023, 2024], dtype=np.dtype("int32")),
      "Country": pd.Series([
          "Australia", "Australia", "Australia",
          "New Zealand", "New Zealand", "New Zealand", "New Zealand", "New Zealand"], dtype=np.dtype("O")),
      "TotalEmissions": pd.Series([2.0, 2.0, 4.0, np.nan, 6.5, 6.5, 7.0, 7.0], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  output_df = boss_battle(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert sorted(output_pandas["Country"].unique()) == sorted(["Australia", "New Zealand"])
  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")

test_boss_battle()

# COMMAND ----------

def test_boss_battle_the_revenge():
  input_pandas = pd.DataFrame({
      "Year": [1997, 2019, 2020, 2022, 2016, 2020, 2022, 2023, 2024],
      "Country" : [
          "United States",
          "Australia", "Australia", "Australia",
          "New Zealand", "New Zealand", "New Zealand", "New Zealand", "New Zealand"],
      "TotalEmissions": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 6.5, 7.0, 8.0],
      "PerCapitaEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.8],
      "ShareOfGlobalEmissions": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.8]
  })
  input_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True)
  ])
  input_df = spark.createDataFrame(input_pandas, input_schema)

  expected_columns = ["Year", "Country", "TotalEmissions"]
  expected_pandas = pd.DataFrame({
      "Year": pd.Series([2016, 2019, 2020, 2020, 2022, 2022, 2023, 2024], dtype=np.dtype("int32")),
      "Country": pd.Series([
          "New Zealand", "Australia", "Australia",
          "New Zealand", "Australia", "New Zealand", "New Zealand", "New Zealand"], dtype=np.dtype("O")),
      "TotalEmissions": pd.Series([np.nan, 2.0, 2.0, 6.5, 4.0, 6.5, 7.0, 7.0], dtype=np.dtype("float32"))
  })
  expected_pandas = prepare_frame(
      expected_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  output_df = boss_battle(input_df)
  output_pandas: pd.DataFrame = output_df.toPandas()
  output_pandas = prepare_frame(
      output_pandas,
      column_order=expected_columns, # ensure column order
      sort_keys=["Year", "Country"], # ensure row order
  )
  print("Schemas:")
  print(expected_pandas.dtypes)
  print(output_pandas.dtypes)
  print("Contents:")
  print(expected_pandas)
  print(output_pandas)

  assert sorted(output_pandas["Country"].unique()) == sorted(["Australia", "New Zealand"])
  assert list(output_pandas.columns) == expected_columns # check column names and order
  assert output_pandas.equals(expected_pandas) # check contents and data types
  print("All tests passed :)")
  
test_boss_battle_the_revenge()  


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's put it together

# COMMAND ----------

oceania_emissions_edited = boss_battle(country_emissions)
display(oceania_emissions_edited)

# COMMAND ----------

oceania_emissions_edited.coalesce(1).orderBy("Year") \
  .write.format("parquet").mode("overwrite") \
  .save(f"{working_directory}/OceaniaEmissionsEdited.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## All together now!
# MAGIC Congrats for completing all four milestones! The only thing that's left is to run one more test to ensure that all expectations are met in each resulting Parquet file.

# COMMAND ----------

def get_expected_metadata():

  co2_temperatures_global_expected_count = 266
  co2_temperatures_global_expected_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("LandAverageTemperature", FloatType(), True),
      StructField("LandMaxTemperature", FloatType(), True),
      StructField("LandMinTemperature", FloatType(), True),
      StructField("LandAndOceanAverageTemperature", FloatType(), True)
      ]
  )

  co2_temperatures_country_expected_count = 18529
  co2_temperatures_country_expected_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True),
      StructField("PerCapitaEmissions", FloatType(), True),
      StructField("ShareOfGlobalEmissions", FloatType(), True),
      StructField("AverageTemperature", FloatType(), True)
      ]
  )

  europe_big_3_co2_expected_count = 120
  europe_big_3_co2_expected_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("France_TotalEmissions", FloatType(), True),
      StructField("France_PerCapitaEmissions", FloatType(), True),
      StructField("Germany_TotalEmissions", FloatType(), True),
      StructField("Germany_PerCapitaEmissions", FloatType(), True),
      StructField("UnitedKingdom_TotalEmissions", FloatType(), True),
      StructField("UnitedKingdom_PerCapitaEmissions", FloatType(), True)
      ]
  )

  co2_oceania_expected_count = 302
  co2_oceania_expected_schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Country", StringType(), True),
      StructField("TotalEmissions", FloatType(), True)
      ]
  )

  expected_output_metadata = {
      "co2_temperatures_global": {"count": co2_temperatures_global_expected_count, "schema": co2_temperatures_global_expected_schema},
      "co2_temperatures_country": {"count": co2_temperatures_country_expected_count, "schema": co2_temperatures_country_expected_schema},
      "europe_big_3_co2": {"count": europe_big_3_co2_expected_count, "schema": europe_big_3_co2_expected_schema},
      "co2_oceania": {"count": co2_oceania_expected_count, "schema": co2_oceania_expected_schema}
  }

  return expected_output_metadata




def test_run():
  """High level job test: count + schema checks but nothing more granular"""

  output_paths = {
    "co2_temperatures_global": f"{working_directory}/GlobalEmissionsVsTemperatures.parquet",
    "co2_temperatures_country": f"{working_directory}/CountryEmissionsVsTemperatures.parquet",
    "europe_big_3_co2": f"{working_directory}/EuropeBigThreeEmissions.parquet",
    "co2_oceania": f"{working_directory}/OceaniaEmissionsEdited.parquet"

  }
  expected_metadata_dict = get_expected_metadata()
  expected_metadata = [expected_metadata_dict[k] for k in output_paths.keys()]

  for (path, expected) in list(zip(output_paths.values(), expected_metadata)):
      files = dbutils.fs.ls(path)
      
      files_df = spark.createDataFrame(files)
      snappy_parquet_files = files_df.filter(files_df.name.endswith('.snappy.parquet'))
      success_files = files_df.filter(files_df.name.endswith('_SUCCESS'))

      assert (True if snappy_parquet_files.count() == 1 else False)
      assert (True if success_files.count() == 1 else False)

     # Check count and schema - this covers most of pyspark-test's (https://pypi.org/project/pyspark-test/) functionality already
     # No need for a full equality check (it collects everything into the driver's memory - too time/memory consuming)
      df = spark.read.parquet(path)
      assert df.count() == expected["count"]
      assert df.schema == expected["schema"]
  
  print("All tests passed :)")
  
test_run()  
