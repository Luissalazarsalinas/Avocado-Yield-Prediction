# Hass Avocado Yield Prediction

Data Science and Data Engineer Freelance Project.

## Problem Statement
### Introduction
Colombia is the fourth largest avocado producer globally and the third largest in terms of harvested area, with a 6% share of
the world area. The development and promotion of this production line represents an important source of growth in agriculture for Colombia, due to the generation of rural employment, equitable development across the different regions of the country, and due to the diversity of thermal floors and the different varieties planted. Imports of this product have decreased by 96% in the last four years from 3128 tons in 2014 to 133 tons in 2017. In recent years, the projects developed in the Colombian avocado sector have been aimed at the foreign market due to the profitability and demand of the Hass variety of this fruit.  In addition,  the per capita consumption of avocado has been growing globally at a rate of 3.5 % per year, in the global context, and it is projected to continue doing so for several years, reaching values of 0.85 (kilograms/person/year). This data showed a promising scenario for high producing countries, which see their agricultural sectors profit as demand increases and incomes rise for producers. However, despite these figures, the country [Colombia] does not have enough information to allow it to apply technological developments to accompany the increase in new areas and safeguard current ones, in order to meet market requirements in fruit quality. The knowledge of the components of the productivity function (genotype, environment),  would allow the country to understand more precisely the response of the species to the tropical conditions in which this fruit tree is currently exploited in the country.

### Goal

Build a data pipeline that integrates environmental data, soil characteristics, and historical avocado yield production data. Perform data preprocessing, feature engineering, and model training to analyze the relationship between environmental factors, soil conditions, and avocado yield. This pipeline can provide insights to avocado growers and agricultural organizations to make informed decisions regarding cultivation practices, resource management, and yield optimization in relation to environmental and soil variables.

### Ojectives

- Design and implement a Data pipeline using the ETL process
- Design and implement a Data Warehouse using Apache Hive
- Design and implement cleaning/transformation strategies using Apache Spark
- Build and train a Machine learning model
- Deploy the machine learning model as a web server using FastApi and Docker
- Build a Unit Test
- Implement a CI/CD pipeline
- Build a Dashboard report with Power Bi

## Data engineering
### Data Pipeline Architecture 
![IMG](https://github.com/Luissalazarsalinas/Avocado-Yield-Prediction/blob/master/images/new_avocado_data_archicteture.png)
Pipeline's architecture

**Ingestion Layer :** a python application that download csv files from open soruce Natational databases and ingest them in raw zone of a data lake (HDFS).

**Transform and Storage Layer :** a spark application and data lake divide into three zones.
    
    - Raw zone: 
       - Activities:
          1. Data ingestion: CSV files were ingestion into the raw zone form Ingestion layer.
          2. Data Storage: the raw data was stored as-is, preserving its original format.

    - Cleaning zone:
       - Activities:
          1. Data ingestion: Data were retrieved from the raw zone using Apache Spark on Databricks
          2. Data Cleaning: 
              - Identify and remove: duplicated and missing values
              - Filter and remove unwanted values
              - replace row values 
              - drop unnecesarie data
          3. Data Transformations:
              - Standarization: rename columns and standarized units
              - Union different files with the same schema
              - Agregations by location (Departamento and Municipio) and date (year)
          4. Data Storage: the clean data were stored in parquet format for optimization and speed. 

    - Curate zone:
       - Activities:
          1. Data ingestion: Data were retrieved from the cleaning zone using Apache Spark on Databricks
          2.  Data Transformation: all data were joined using join operation with Apache spark.
          3. Partitioning: Data were partitioned by Crops to optimize the query performance.

**Serving Layer :** a Apache Hive datawarehouse to stored relational data from curate zone.

**Orchestration Layer :**


**Consumer :**


## Data Science
### Data preparation
[document all the process of preprocessing steps(cleaning, variables' transformation, feature engineering and feature selection)]

### Modeling 
[list of models that i used]
[Metrics of performance]

#### Final model
##### Hyperparameters optimization
[best model its performance]

##### Main Features
[the features with greater importance for the model]

### Deployment/Web service
- Deploy the model as a Rest api using FastApi
- add validation schema
- add security to the add [JWT]
- connect the api with a PostgreSQL database to store login and prediction from the model
- Add unit test and CI pipeline

