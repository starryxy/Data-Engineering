# Sparkify: Data Lake with Spark Project <br>

## Summary

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database even more want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Task**: build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in partitioned parquet files


## Database Schema

**Datasets & Location**: <br>
Song data: `s3://udacity-dend/song_data` <br>
Log data: `s3://udacity-dend/log_data` <br>


The star schema includes 1 Fact Table and 4 Dimension Tables. 

**Fact Table**

- `songplays` <br>
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

- `users` <br>
user_id, first_name, last_name, gender, level

- `songs` <br>
song_id, title, artist_id, year, duration

- `artists` <br>
artist_id, name, location, latitude, longitude

- `time` <br>
start_time, hour, day, week, month, year, weekday


## Files in Repository

- `README.md` <br>
brief intro of the project
- `dl.cfg` <br>
config file that contains AWS credentials, and S3 path for output files
- `etl.py` <br>
script to extract JSON data from the S3 bucket, process them using Spark, and load the data back into S3 as a set of dimensional tables in partitioned parquet files
- `test.ipynb` <br>
test whether all tables data are accessible with sample queries


## How to Run

1. Create an IAM role with `AmazonS3FullAccess` policy attached
    
2. Copy Access key and Secret access key of the IAM role to `dl.cfg` file; put your S3 path for output files

3. Execute below command in Python console to execute the ETL process:
    `run etl.py`

4. Once you have 5 tables written in partitioned parquet files on S3, open `test.ipynb` file, test whether all tables' data is accessible by running the cells to read the files and test sample queries

