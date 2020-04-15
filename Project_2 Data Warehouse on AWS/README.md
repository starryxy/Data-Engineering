# Sparkify: Data Warehouse on AWS Project

## Summary

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Task**: build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables


## Database Schema

**Datasets & Location**: <br>
Song data: `s3://udacity-dend/song_data` <br>
Log data: `s3://udacity-dend/log_data` <br>
Log data json path: `s3://udacity-dend/log_json_path.json`


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
- `dwh.cfg` <br>
config file that contains info of cluster, redshift database iam role, and datasets S3 locations
- `sql_queries.py` <br>
contain SQL queries for DROP and CREATE tables, COPY and INSERT into tables
- `create_tables.py` <br>
script to drop old tables if exist and create new tables in Redshift
- `etl.py` <br>
script to extract JSON data from the S3 bucket, load data to staging tables, transform data and load into fact and dimension tables on Redshift


## How to Run

1. Create an AWS Redshift `dc2.large` Cluster with **4 nodes**. Create an IAM role with `AmazonS3ReadOnlyAccess` policy attached

2. Create a configuration file with the file name `dwh.cfg` as the following structure:

```
[CLUSTER]
HOST = [your_host]
DB_NAME = [your_db_name]
DB_USER = [your_db_user]
DB_PASSWORD = [your_db_password]
DB_PORT = [your_db_port]
DB_REGION = [your_db_region]
CLUSTER_IDENTIFIER = [your_cluster_identifier]

[IAM_ROLE]
ARN = [your_iam_role_arn]

[S3]
LOG_DATA = 's3://udacity-dend/log_data'
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'
SONG_DATA = 's3://udacity-dend/song_data'

[AWS]
ACCESS_KEY = [your_access_key]
SECRET_KEY = [your_secret_key]
```


3. Execute below commands in Python console:

    To create fact and dimension tables <br>
    `run create_tables.py`

    To execute ETL process <br>
    `run etl.py`

4. Remember to delete the Redshift Cluster you created when you finished to avoid unnecessary cost


## Query Example

Once you've created the Redshift database and run the ETL pipeline, you can test out some queries in Redshift console query editor.

- Find total number of songs users listened to for the whole time
```sql
SELECT COUNT(DISTINCT title) AS num_songs 
FROM songs;

Result:
num_songs
14402
```

- Find number of users by gender
```sql
SELECT gender, COUNT(DISTINCT user_id) AS cnt 
FROM users 
GROUP BY gender;

Result: 
gender   cnt
F        55
M        41
```

- Top artists by number of song plays
```sql
SELECT a.name, count(a.name) AS num_songplays
FROM songplays s
LEFT JOIN artists a 
ON s.artist_id = a.artist_id
GROUP BY a.name
ORDER BY num_songplays DESC
LIMIT 3;
    
Result: 
name           num_songplays
Muse           42
Dwight Yoakam  37
Radiohead      24
```
