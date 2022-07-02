# CO2 vs. Temperature Exercise (Databricks)
This repository contains exercises in Databricks that ingests Global Temperature and Global Temperature By Country data from Kaggle and CO2 Emissions data from OWID and transforms it. The goal of this exercise is to teach some basics about data wrangling and Spark with respect to real world questions.

* Which countries are worse-hit (higher temperature anomalies)?
* Which countries are the biggest emitters?
* What are some attempts of ranking “biggest polluters” in a sensible way?

## Data Sources
In order to answer some of the questions of the exercise, we picked open-source data from [Open World in Data (OWID)](https://github.com/owid/owid-datasets/tree/0f47d280d298694c50b82db98daa94cd6e867d2e/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2020))) and [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

The specific datasets:
* [CO2 Emissions (2020).csv (OWID)](https://github.com/owid/owid-datasets/blob/0f47d280d298694c50b82db98daa94cd6e867d2e/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2020))/CO2%20emissions%20(Aggregate%20dataset%20(2020)).csv)
* [GlobalLandTemperaturesByCountry (Kaggle)](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCountry.csv)
* [GlobalTemperatures.csv (Kaggle)](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalTemperatures.csv)

## Data Sources (Modified!)
Since the point of this exercise is to learn how to work with data and the datasets from OWID and Kaggle are both too clean and curated, a set of dirtied data is provided.

They can be found at:
* [EmissionsByCountry.csv](https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/EmissionsByCountry.csv)
* [GlobalTemperatures.csv](https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/GlobalTemperatures.csv)
* [TemperaturesByCountry.csv](https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature/master/data-ingestion/input-data/TemperaturesByCountry.csv)

## Prerequisites
* Basic knowledge of Python
* Basic knowledge of Spark
* [Databricks Account](#create-a-databricks-community-account)

## Quickstart
1. Set up a [Databricks Account](https://github.com/data-derp/documentation/blob/master/databricks/README.md)
2. [Create a cluster](https://github.com/data-derp/documentation/blob/master/databricks/setup-cluster.md)

### Data Ingestion
1. In your User's workspace, click import
   
   ![databricks-import](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-import.png?raw=true)

2. Import the `Data Ingestion CO2 vs Temperature.py` notebook: `https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-ingestion/Data%20Ingestion%20CO2%20vs%20Temperature.py`
   
   ![databricks-import-url](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-import-url.png?raw=true)

3. Select your cluster

   ![databricks-select-cluster.png](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-select-cluster.png?raw=true)

4. Follow instructions, move on to following exercises once all tests pass.

5. Once you're done, import the solutions `Data Ingestion CO2 vs Temperature Solutions.py` notebook to check your answers: `https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-ingestion/Data%20Ingestion%20CO2%20vs%20Temperature%20Solutions.py`

### Data Transformation
1. In your User's workspace, click import

   ![databricks-import](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-import.png?raw=true)

2. Import the `Data Transformation CO2 vs Temperature.py` notebook: `https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/Data%20Transformation%20CO2%20vs%20Temperature.py`

   ![databricks-import-url](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-import-url.png?raw=true)

3. Select your cluster

   ![databricks-select-cluster.png](https://github.com/data-derp/documentation/blob/master/databricks/assets/databricks-select-cluster.png?raw=true)

4. Follow instructions, move on to following exercises once all tests pass.

5. Once you're done, import the solutions `Data Ingestion CO2 vs Temperature Solutions.py` notebook to check your answers: `https://raw.githubusercontent.com/data-derp/exercise-co2-vs-temperature-databricks/master/data-transformation/Data%20Transformation%20CO2%20vs%20Temperature%20Solutions.py`

## Developing
When uploading new datasets from a Mac, you might have to run `zip -d input-data.zip __MACOSX/\*` after zipping so that this hidden file gets removed