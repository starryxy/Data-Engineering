# Sparkify: Data Lake with Spark Project <br>

## Summary

A startup, Sparkify, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database even more want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Task**: build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in partitioned parquet files

<br>

## Database Schema

**Datasets & Location**: <br>
Song data: <code>s3://udacity-dend/song_data</code> <br>
Log data: <code>s3://udacity-dend/log_data</code> <br>


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
- <code>dl.cfg</code> <br>
<small>config file that contains AWS credentials, and S3 path for output files</small>
- <code>etl.py</code> <br>
<small>script to extract JSON data from the S3 bucket, process them using Spark, and load the data back into S3 as a set of dimensional tables in partitioned parquet files</small>
- <code>test.ipynb</code> <br>
<small>test whether all tables data are accessible with sample queries</small>

<br>

## How to Run
<ol>
<li>Create an IAM role with <code>AmazonS3FullAccess</code> policy attached</li>
    
<li>Copy Access key and Secret access key of the IAM role to <code>dl.cfg</code> file; put your S3 path for output files</li>

<li>Execute below command in Python console to execute the ETL process:</li>
    <code>run etl.py</code>

<li>Once you have 5 tables written in partitioned parquet files on S3, open <code>test.ipynb</code> file, test whether all tables' data is accessible by running the cells to read the files and test sample queries</li>
    
</ol>

