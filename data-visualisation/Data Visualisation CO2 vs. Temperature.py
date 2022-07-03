# Databricks notebook source
# MAGIC %md
# MAGIC # Data Visualisation - CO2 vs. Temperature
# MAGIC Now that we have data in a desired shape, let's visualise it using different visualisation libraries in Python!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install some dependencies

# COMMAND ----------

# MAGIC %pip install pandas==1.2 s3fs plotly scikit-learn==0.24.2 dash wget

# COMMAND ----------

# Clear out existing working directory

current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/dataVisualisation"
dbutils.fs.rm(working_directory, True)

# COMMAND ----------

# Function that checks that files exist from the previous exercise

def contains_expected_files(dir, expected_files):
    match_expected_extensions=lambda x: x.endswith(tuple(expected_files))
    get_name=lambda x: x.name
    try: 
        num_matching_files = len(list(filter(match_expected_extensions, map(get_name, dbutils.fs.ls(dir)))))
        return num_matching_files >= len(expected_ingestion_parquet_files)
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# Function to download files to DBFS

import os
import wget
import sys
import shutil

sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

def clean_remake_dir(dir):
    if os.path.isdir(local_tmp_dir): shutil.rmtree(local_tmp_dir)
    os.makedirs(local_tmp_dir)
    

def download_to_local_dir(local_dir, target_dir, url, filename_parsing_lambda):
    filename = (filename_parsing_lambda)(url)
    tmp_path = f"{local_dir}/{filename}"
    target_path = f"{target_dir}/{filename}"
    if os.path.exists(tmp_path):
        os.remove(tmp_path) 
    
    saved_filename = wget.download(url, out = tmp_path)
    
    if target_path.endswith(".zip"):
        with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
            zip_ref.extractall(local_dir)

    dbutils.fs.cp(f"file:{local_dir}/", target_dir, True)
    
    return target_path

# COMMAND ----------

# If prerequisite files don't already exist, download them

import zipfile


expected_ingestion_parquet_dirs = [
    f"/FileStore/{current_user}/dataTransformation/CountryEmissionsVsTemperatures.parquet/",
    f"/FileStore/{current_user}/dataTransformation/EuropeBigThreeEmissions.parquet/",
    f"/FileStore/{current_user}/dataTransformation/GlobalEmissionsVsTemperatures.parquet/",
    f"/FileStore/{current_user}/dataTransformation/OceaniaEmissionsEdited.parquet/"
]

expected_ingestion_parquet_files = [
    "_SUCCESS",
    ".parquet"
]

expected_files_exist = all(list(map(lambda x: contains_expected_files(x, expected_ingestion_parquet_files), expected_ingestion_parquet_dirs)))

source_directory = f"/FileStore/{current_user}/dataTransformation"

if expected_files_exist:
    print("Prerequisite files exist. Nothing to do here!")
else:
    print("Prerequisite files don't yet exist. Downloading...")
    
    local_tmp_dir = f"{os.getcwd()}/{current_user}/dataTransformation/tmp"
    clean_remake_dir(local_tmp_dir)

    url = "https://github.com/data-derp/exercise-co2-vs-temperature-visualisation/blob/master/transformation-outputs/transformation-outputs.zip?raw=true"
    
    download_to_local_dir(local_tmp_dir, source_directory, url, lambda y: y.split("/")[-1].replace("?raw=true",""))
    
    
display(spark.createDataFrame(dbutils.fs.ls(source_directory)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read Parquet Data from our Data Transformation Output
# MAGIC Let's read our data from our Data Transformation output. Note: behind the scenes, this exists in AWS S3 blob storage. Remember, that typically, there would be some access layer in between you and the data. Right now, we're reading non-sensitive data from our DBFS storage which is in an S3 bucket (see `storage_options`) and [other examples](https://docs.dask.org/en/stable/how-to/connect-to-remote-data.html).

# COMMAND ----------

europe_big_three_emissions_df = spark.read.parquet(f"{source_directory}/EuropeBigThreeEmissions.parquet/")
display(europe_big_three_emissions_df)

# COMMAND ----------

## If you would like to experiment with Machine Learning
import sklearn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotly Examples
# MAGIC Plotly is a common tool for data visualisation. Let's have a look at how it works.
# MAGIC 
# MAGIC Examples from [Plotly](https://plotly.com/python/getting-started/)

# COMMAND ----------

import plotly.express as px
fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
fig.write_html('first_figure.html', auto_open=True)
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Visualise CO2 vs. Temperature Data
# MAGIC Use Plotly to play around with the project data to see if you can come up with meaningful visualisations that answer our original questions:
# MAGIC * Which countries are worse-hit (higher temperature anomalies)?
# MAGIC * Which countries are the biggest emitters?
# MAGIC * What are some attempts of ranking “biggest polluters” in a sensible way?

# COMMAND ----------


