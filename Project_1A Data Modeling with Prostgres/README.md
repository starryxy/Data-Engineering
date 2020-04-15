# Sparkify: Data Modeling with Postgres Project <br>

## Summary

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

**Goal**: understand what songs users are listening to

**Task**: create a star schema and ETL pipeline


## Database schema design and ETL pipeline

### Datasets used: song_data, log_data

The star schema includes 1 Fact Table and 4 Dimension Tables. 

### Fact Table

- *songplays* <br>
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

- *users* <br>
user_id, first_name, last_name, gender, level

- *songs* <br>
song_id, title, artist_id, year, duration

- *artists* <br>
artist_id, name, location, latitude, longitude

- *time* <br>
start_time, hour, day, week, month, year, weekday


## Files in the repository

- *README.md* <br>
brief intro of the project

- *data* <br>
folder that contains song and log datasets

- *sql_queries.py* <br>
contains SQL queries for DROP and CREATE tables, INSERT rows

- *create_tables.py* <br>
creates database sparkifydb, imports sql_queries module to drop and create tables

- *etl.ipynb* <br>
develops the ETL process on a single file in song_data or log_data database and load into fact or dimension table

- *etl.py* <br>
develops the ETL process and load the whole song_data and log_data datasets based on etl.ipynb

- *test.ipynd* <br>
test whether tables are created and whether records are added to according table


## How to run the Python scripts

Execute below commands in Python console: 

To create database sparkifydb, and create fact and dimension tables <br>
    `run create_tables.py`

To load whole song_data and log_data datasets into tables <br>
    `run etl.py`


## Example queries and results

1. Find total number of songs users listened to for the whole time
```sql
%sql SELECT COUNT(DISTINCT title) AS num_songs \
     FROM songs;

Result: 
num_songs
71
```

2. Find number of users by gender
```sql
%sql SELECT gender, COUNT(DISTINCT user_id) AS cnt \
     FROM users \
     GROUP BY gender;

Result: 
gender   cnt
F        55
M        41
```

3. Find first name, last name, and gender of Top 5 users who have the highest number of song played times
```sql
%sql SELECT t2.first_name, t2.last_name, t2.gender, t1.cnt \
     FROM \
     ( \
        SELECT user_id, COUNT(user_id) AS cnt \
        FROM songplays \
        GROUP BY user_id \
     ) t1 \
     JOIN users t2 ON t1.user_id = t2.user_id \
     ORDER BY t1.cnt DESC \
     LIMIT 5;

Result: 
first_name   last_name   gender   cnt
Chloe        Cuevas      F        689
Tegan        Levine      F        665
Kate         Harrell     F        557
Lily         Koch        F        463
Aleena       Kirby       F        397
```
