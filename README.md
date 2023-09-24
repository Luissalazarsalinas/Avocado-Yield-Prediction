# Hass Avocado Yield Prediction
[Space for badges](url)

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
- Containerizing ETL pipeline with Docker
- Build and train a Machine learning model
- Deploy the machine learning model as a web server using FastApi and Docker
- Build a Dashboard report with Power Bi

*** In a fucture add objectives of add security layer to the ETL pipeline and unit test to the pipeline and the model web server code


## Data egineering part
### Data Pipeline Architecture 
![IMG](https://github.com/Luissalazarsalinas/Avocado-Yield-Prediction/blob/master/images/Data-Architecture.jpg)

All applications were containerized into Docker containers.

**Data sources :**


**Ingestion Layer :**

**Transform and Storage Layer :**

**Data Catologing :**

**Serving Layer :**


**Orchestration Layer :**


**Consumer :**


## Data Science part
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

## Data analytics part
[Create a dashboad that comsumed the data from the data pipeline using Power Bi]
[add the image of the dashboard]

