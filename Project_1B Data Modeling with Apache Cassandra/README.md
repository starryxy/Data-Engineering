# Sparkify: Data Modeling with Apache Cassandra Project <br>

## Summary

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

**Task**: create an Apache Cassandra database


## Files in the repository

- `README.md` <br>
brief intro of the project

- `event_data` <br>
folder that contains event dataset

- `images` <br>
folder that contains an image inserted in etl.ipynb file

- `event_datafile_new.csv` <br>
a .csv file created in etl.ipynb file that will be used to insert data into the Apache Cassandra tables

- `etl.ipynb` <br>
develops the ETL process which extracts data from event_data dataset, transforms and load into event_datafile_new.csv file; then uses event_datafile_new.csv file to create tables and queries to answer some analytical questions
