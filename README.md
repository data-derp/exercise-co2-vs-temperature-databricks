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

## Exercises
1. [Data Ingestion](./data-ingestion/README.md)
2. [Data Transformation](./data-transformation/README.md)
3. [Data Visualisation](./data-visualisation/README.md)
4. [Grand Finale](./finale/README.md)

## Contributing
### Some hints
* It's easiest to make changes to the Solutions notebooks, then clone it, remove the code blocks clearly marked solutions
* When uploading new datasets from a Mac, you might have to run `zip -d input-data.zip __MACOSX/\*` after zipping so that this hidden file gets removed