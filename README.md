# Ener-See

In this project, a data pipline is constructed to monitor real-time energy consumption distribution of commercial buildings and generate alert.

![alt text](https://github.com/siyu1/insight-engineering-project-energy-consumption/blob/master/figures/Screenshot%202018-05-13%2022.32.32.png)

slides
https://docs.google.com/presentation/d/1yb7b6tzQi1ox9iNdbhb3ild454aD2lf7Uc4EvD7u4vQ/edit#slide=id.p

Link:
http://ec2-52-13-146-195.us-west-2.compute.amazonaws.com/



# User Case
Advanced monitoring system is more and more widely applied in commercial buildings nowadays. In the near future, huge amount of data will be generated everyday from those buildings. If we can build a great platform and make good use of these data, our world will become more energy efficient. This project provides a data pipline to compare the detailed real-time energy consumption data of similar type of commercial buildings within the same area. Also, a real-time "alert" is genrated for those buildings which are cosuming much more nergies than similar type of buildings. This platform can effectively help improve energy efficiency, which can be very useful for the utility companoies and also the business owners.



# Data Set
Commercial and Residential Hourly Load Profiles for all TMY3 Locations in the United States 
https://catalog.data.gov/dataset/commercial-and-residential-hourly-load-profiles-for-all-tmy3-locations-in-the-united-state
1. Time range: 2004~ 2015
2. Number of buisiness
16 different bussiness types from different locations, 3000 buildings, can be expanded to larger dataset
2. Parameters: 
Electricity (lighting, facilities, cooling, heating, fan, equipments)
Gas (heating, water, equipments, facilities)

 
# Technologies

Data Ingestion Layer: Kafka (3 nodes)

Streaming Process: Spark streaming (3 nodes)

DataBase: Canssandra (4 nodes)

# Proposed structure

![alt text](https://github.com/siyu1/insight-engineering-project-energy-consumption/blob/master/figures/Screenshot%202018-05-07%2014.35.05.png)

# Stream processing- Spark
In this work, stream processing is acheived using Spark streaming. Spark streaming process the data by micro-batches, which is not "truly real-time processing". However, in this user case, the energy-consumption analysis results doesn't require truly real-time response, which allows a small latency. Also, processing the data by micro batches allows us to tune the computation peformance by changing the micro-batch time period, which can be quite useful for this project, since the platform is designed for huge-amount of energy consumption data ingestion and processing.

# Database- Cassandra
In this work, Cassandra is used as database. It allows fast-writing and partition the data by a "Partition Key". I chose the location of the building as the partition key in this project, and all the buildings within the same area are stored in the same node, this allows very fast quering for the end users. Also,Cassandra's column-oriented architecture is a good fit for time-series data. The query is very fast if we want to access the historical data for any building. 

# Performance optimization

Tuning micro-batch time 

Spark-streaming allows us to set the time period of the micro batches. In this work, I changed the time periode and tested the system performance. At the beginning, it was found that for smaller incoming data stream (~ 1500 records/s), increasing the micro batch time doesn't influence the total processing time too much, and using longer micro batch time makes the processing more efficient. However, when there is larger incoming data stream coming (~ 5000 records/s), increasing the micro batch time dramatically influenced the total processing time (shown in the following figure).

![alt text](https://github.com/siyu1/insight-engineering-project-energy-consumption/blob/master/figures/Screenshot%202018-05-13%2021.34.52.png)

After investigating the processing time in detail, it was found that the map functions in the streaming process affected the processing time a lot when the incoming stream becomes larger. To solve this problem, I avoided using map functions and performed the data transforming when saving the data to Cassandra database. This effectively reduced the total processing time for longer micro batch time (indicated in the following figure). The total processing time decreased by 60% for the 20s batch.

![alt text](https://github.com/siyu1/insight-engineering-project-energy-consumption/blob/master/figures/Screenshot%202018-05-13%2021.35.19.png)

# Directory Structure

Spark - PySpark code for performing calculations and saving to database

Cassandra - CQL commands to set up keyspaces and tables in Cassandra

figures - Images for this README

flask - Code for web app

Kafka- Code for kafka data ingestion



