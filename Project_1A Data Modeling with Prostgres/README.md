# Sparkify: Data Modeling with Postgres Project <br>

## Summary

A startup, Sparkify, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

**Goal**: understand what songs users are listening to

**Task**: create a star schema and ETL pipeline

<br>

## Database schema design and ETL pipeline

**Datasets used**: song_data, log_data

The star schema includes 1 Fact Table and 4 Dimension Tables. 

**Fact Table**

- *songplays* <br>
<small>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</small>

**Dimension Tables**

- *users* <br>
<small>user_id, first_name, last_name, gender, level</small>

- *songs* <br>
<small>song_id, title, artist_id, year, duration</small>

- *artists* <br>
<small>artist_id, name, location, latitude, longitude</small>

- *time* <br>
<small>start_time, hour, day, week, month, year, weekday</small>

<br>

## Files in the repository

- *README.md* <br>
<small>brief intro of the project</small>
- *data* <br>
<small>folder that contains song and log datasets</small>
- *sql_queries.py* <br>
<small>contains SQL queries for DROP and CREATE tables, INSERT rows</small>
- *create_tables.py* <br>
<small>creates database sparkifydb, imports sql_queries module to drop and create tables</small>
- *etl.ipynb* <br>
<small>develops the ETL process on a single file in song_data or log_data database and load into fact or dimension table</small>
- *etl.py* <br>
<small>develops the ETL process and load the whole song_data and log_data datasets based on etl.ipynb</small>
- *test.ipynd* <br>
<small>test whether tables are created and whether records are added to according table</small>

<br>

### How to run the Python scripts

Execute below commands in Python console: 

To create database sparkifydb, and create fact and dimension tables <br>
    `run create_tables.py`

To load whole song_data and log_data datasets into tables <br>
    `run etl.py`

<br>

## Example queries and results for song play analysis

1. Find total number of songs users listened to for the whole time
```sql
%sql SELECT COUNT(DISTINCT title) AS num_songs \
        FROM songs;
```
<br>
    <small>Result: <br>
    num_songs<br>
    71</small>


2. Find number of users by gender
```sql
%sql SELECT gender, COUNT(DISTINCT user_id) AS cnt \
        FROM users \
        GROUP BY gender;
```
<br>
    <small>Result: <br>
    gender   cnt<br>
    F        55<br>
    M        41</small>


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
```
<br>
    <small>Result: <br>
    first_name   last_name   gender   cnt<br>
    Chloe        Cuevas      F        689<br>
    Tegan        Levine      F        665<br>
    Kate         Harrell     F        557<br>
    Lily         Koch        F        463<br>
    Aleena       Kirby       F        397</small>

