# insight-engineering-project-energy-consumption
# Introduction
Monitor and analyze the energy consumption of commercial buildings

# User Case
Advanced monitoring system is more and more widely applied in commercial buildings nowadays. By 2020, 70% of the buildings in London will be installed with smart meters. This project provides the bussiness owners the capability to evaluate and compare the energy consumption, to improve the energy efficiency. The business owners can continuously monitor the different types of energy consumption and compare with other similar businees around the area. This can effectively help them improve the energy efficiency. Both Real-time analysis and batch analytics based on historical data are needed.

# Data Set
Commercial and Residential Hourly Load Profiles for all TMY3 Locations in the United States (20 GB)
https://catalog.data.gov/dataset/commercial-and-residential-hourly-load-profiles-for-all-tmy3-locations-in-the-united-state
1. Time range: 2004~ 2015
2. Number of buisiness
16 different bussiness types from different locations, 1505 buildings, can be expanded to larger dataset
2. Parameters: 
Electricity (lighting, airconditioning)
Gas
Water
 
# Proposed structure

Data Storage Layer: Common Crawl, AWS S3

Data Ingestion Layer: Kafka

Streaming Process: Spark streaming, Flink

Bash Process: Spark 

DataBase: Canssandra, Amazon Redshift

![](https://github.com/siyu1/insight-engineering-project-energy-consumption/blob/master/Screenshot%202018-04-20%2012.11.47.png)
