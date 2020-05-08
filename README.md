# Data Engineering Projects

<br>
This repo contains 6 projects for Udacity's Data Engineering Nanodegree: 

<br>

- ### [Data Modeling with Postgres](https://github.com/starryxy/Data-Engineering/tree/master/Project_1A%20Data%20Modeling%20with%20Prostgres)
Create a star schema and ETL pipeline with Postgres

- ### [Data Modeling with Apache Cassandra](https://github.com/starryxy/Data-Engineering/tree/master/Project_1B%20Data%20Modeling%20with%20Apache%20Cassandra)
Create an Apache Cassandra database

- ### [Data Warehouse on AWS](https://github.com/starryxy/Data-Engineering/tree/master/Project_2%20Data%20Warehouse%20on%20AWS)
Build an ETL pipeline that extracts songs and user activity data from S3, stages them in Redshift, and transforms data into fact and dimension tables

- ### [Data Lake with Spark](https://github.com/starryxy/Data-Engineering/tree/master/Project_3%20Data%20Lake%20with%20Spark)
Build an ETL pipeline for a data lake by extracting data from S3, processing them into analytics tables using Spark, and loading them back into S3 as a set of dimensional tables in partitioned parquet files

- ### [Data Pipelines with Airflow](https://github.com/starryxy/Data-Engineering/tree/master/Project_4%20Data%20Pipeline%20with%20Airflow)
Created and automated a data pipeline with DAG which has custom operators to perform tasks that can stage the data, transform the data in data warehouse, and run checks on data quality

- ### Capstone project - TBD

<br>

## About Datasets

All projects used data of Sparkify, a music streaming app.

- ### Song Dataset

A subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song.

The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in the dataset:
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- ### Log Dataset

Log files in JSON format generated by [this event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in the dataset:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Below is an example of what the data in a log file, `2018-11-12-events.json`, looks like:
![](Image/log-data.png)
