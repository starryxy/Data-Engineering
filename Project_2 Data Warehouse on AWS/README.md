# Sparkify: Data Warehouse on AWS Project <br>

## Summary

A startup, Sparkify, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Task**: build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables

<br>

## Database Schema

**Datasets & Location**: <br>
Song data: <code>s3://udacity-dend/song_data</code> <br>
Log data: <code>s3://udacity-dend/log_data</code> <br>
Log data json path: <code>s3://udacity-dend/log_json_path.json</code>


The star schema includes 1 Fact Table and 4 Dimension Tables. 

**Fact Table**

- <code>songplays</code> <br>
<small>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</small>

**Dimension Tables**

- <code>users</code> <br>
<small>user_id, first_name, last_name, gender, level</small>

- <code>songs</code> <br>
<small>song_id, title, artist_id, year, duration</small>

- <code>artists</code> <br>
<small>artist_id, name, location, latitude, longitude</small>

- <code>time</code> <br>
<small>start_time, hour, day, week, month, year, weekday</small>

<br>

## Files in Repository

- <code>README.md</code> <br>
<small>brief intro of the project</small>
- <code>dwh.cfg</code> <br>
<small>config file that contains info of cluster, redshift database iam role, and datasets S3 locations</small>
- <code>sql_queries.py</code> <br>
<small>contain SQL queries for DROP and CREATE tables, COPY and INSERT into tables</small>
- <code>create_tables.py</code> <br>
<small>script to drop old tables if exist and create new tables in Redshift</small>
- <code>etl.py</code> <br>
<small>script to extract JSON data from the S3 bucket, load data to staging tables, transform data and load into fact and dimension tables on Redshift</small>

<br>

## How to Run
<ol>
<li>Create an AWS Redshift <code>dc2.large</code> Cluster with <strong>4 nodes</strong>. Create an IAM role with <code>AmazonS3ReadOnlyAccess</code> policy attached </li>

<li>Create a configuration file with the file name <code>dwh.cfg</code> as the following structure: </li>

<code>
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
</code>


<li>Execute below commands in Python console: </li>

To create fact and dimension tables <br>
    <code>run create_tables.py</code>

To execute ETL process <br>
    <code>run etl.py</code>

<li>Remember to delete the Redshift Cluster you created when you finished to avoid unnecessary cost</li>

</ol>
    
<br>

## Query Example

Once you've created the Redshift database and run the ETL pipeline, you can test out some queries in Redshift console query editor:

<code>
-- Find total number of songs users listened to for the whole time

```SELECT COUNT(DISTINCT title) AS num_songs 
FROM songs;```

<small>Result:
num_songs
14402</small>
</code>

<code>
-- Find number of users by gender

```SELECT gender, COUNT(DISTINCT user_id) AS cnt 
FROM users 
GROUP BY gender;```

<small>Result: 
gender   cnt
F        55
M        41</small>
</code>
    
<code>
--Top artists by number of song plays
    
```SELECT a.name, count(a.name) AS num_songplays
FROM songplays s
LEFT JOIN artists a 
ON s.artist_id = a.artist_id
GROUP BY a.name
ORDER BY num_songplays DESC
LIMIT 3;```
    
<small>Result: 
name           num_songplays
Muse           42
Dwight Yoakam  37
Radiohead      24</small>
    
</code>
