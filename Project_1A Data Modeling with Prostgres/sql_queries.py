# DROP TABLES

songplay_table_drop = " drop table if exists songplays; "
user_table_drop = " drop table if exists users; "
song_table_drop = " drop table if exists songs; "
artist_table_drop = " drop table if exists artists; "
time_table_drop = " drop table if exists time; "

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays \
( \
    songplay_id serial PRIMARY KEY, \
    start_time timestamp NOT NULL, \
    user_id int NOT NULL, \
    level varchar NOT NULL, \
    song_id varchar, \
    artist_id varchar, \
    session_id int NOT NULL, \
    location text, \
    user_agent text \
    ); 
""")

user_table_create = ("""
create table if not exists users \
( \
    user_id int PRIMARY KEY, \
    first_name varchar, \
    last_name varchar, \
    gender varchar, \
    level varchar NOT NULL \
); 
""")

song_table_create = (""" 
create table if not exists songs \
( \
    song_id varchar PRIMARY KEY, \
    title varchar NOT NULL, \
    artist_id varchar NOT NULL, \
    year int NOT NULL, \
    duration float NOT NULL \
); 
""")

artist_table_create = (""" 
create table if not exists artists \
( \
    artist_id varchar PRIMARY KEY, \
    name varchar NOT NULL, \
    location text, \
    latitude float, \
    longitude float \
); 
""")

time_table_create = ("""
create table if not exists time \
( \
    start_time timestamp PRIMARY KEY, \
    hour int NOT NULL, \
    day int NOT NULL, \
    week int NOT NULL, \
    month int NOT NULL, \
    year int NOT NULL, \
    weekday int NOT NULL \
); 
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
values (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) \
values (%s, %s, %s, %s, %s) \
on conflict (user_id) \
do update \
    set first_name = excluded.first_name, \
        last_name = excluded.last_name, \
        gender = excluded.gender, \
        level = excluded.level \
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) \
values (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) \
values (%s, %s, %s, %s, %s) \
on conflict (artist_id) \
DO NOTHING
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday) \
values (%s, %s, %s, %s, %s, %s, %s) \
on conflict (start_time) \
DO NOTHING
""")

# FIND SONGS

song_select = ("""
select t1.song_id, t1.artist_id \
from songs t1 \
left join artists t2 \
    on t1.artist_id = t2.artist_id \
where t1.title = %s \
    and t2.name = %s \
    and t1.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]