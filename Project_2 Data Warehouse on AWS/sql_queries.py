import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP STAGING, FACT, AND DIMENSION TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE STAGING, FACT, AND DIMENSION TABLES

staging_events_table_create= ("""
    create table if not exists staging_events \
    ( \
        artist varchar, \
        auth varchar, \
        firstName varchar, \
        gender varchar, \
        itemInSession int, \
        lastName varchar, \
        length numeric, \
        level varchar, \
        location varchar, \
        method varchar, \
        page varchar, \
        registration varchar, \
        sessionId int, \
        song varchar, \
        status int, \
        ts bigint, \
        userAgent varchar, \
        userId int \
    );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs \
    ( \
        artist_id varchar, \
        artist_latitude numeric, \
        artist_location varchar, \
        artist_longitude numeric, \
        artist_name varchar, \
        duration numeric, \
        num_songs int, \
        song_id varchar, \
        title varchar, \
        year int \
    ); 
""")

songplay_table_create = ("""
    create table if not exists songplays \
    ( \
        songplay_id int IDENTITY(0,1) primary key, \
        start_time timestamp NOT NULL, \
        user_id int NOT NULL, \
        level varchar NOT NULL, \
        song_id varchar, \
        artist_id varchar, \
        session_id int NOT NULL, \
        location varchar, \
        user_agent varchar \
    ); 
""")

user_table_create = ("""
    create table if not exists users \
    ( \
        user_id int primary key, \
        first_name varchar, \
        last_name varchar, \
        gender varchar, \
        level varchar NOT NULL \
    ); 
""")

song_table_create = ("""
    create table if not exists songs \
    ( \
        song_id varchar primary key, \
        title varchar NOT NULL, \
        artist_id varchar NOT NULL, \
        year int NOT NULL, \
        duration double precision NOT NULL \
    ); 
""")

artist_table_create = ("""
    create table if not exists artists \
    ( \
        artist_id varchar primary key, \
        name varchar NOT NULL, \
        location varchar, \
        latitude double precision, \
        longitude double precision \
    ); 
""")

time_table_create = ("""
    create table if not exists time \
    ( \
        start_time timestamp primary key, \
        hour int NOT NULL, \
        day int NOT NULL, \
        week int NOT NULL, \
        month int NOT NULL, \
        year int NOT NULL, \
        weekday int NOT NULL \
    ); 
""")

# LOAD DATA TO STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    region 'us-west-2'
    json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs 
    from {}
    iam_role {}
    region 'us-west-2'
    json 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# LOAD DATA TO FACT AND DIMENSION TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    select timestamp 'epoch' + t1.ts/1000 * interval '1 second', \
            t1.userId, t1.level, t2.song_id, t2.artist_id, t1.sessionId, t1.location, t1.userAgent \
    from staging_events t1 \
    left join staging_songs t2 \
    on t1.artist = t2.artist_name and t1.song = t2.title \
    where t1.page = 'NextSong';
""")

user_table_insert = ("""
    insert into users \
    select t1.userId, t1.firstName, t1.lastName, t1.gender, t1.level \
    from staging_events t1 \
    join ( \
        select userId, max(ts) as ts \
        from staging_events \
        where page = 'NextSong' \
        group by userId \
        ) t2 \
    on t1.userId = t2.userId and t1.ts = t2.ts;
""")

song_table_insert = ("""
    insert into songs \
    select song_id, title, artist_id, year, duration \
    from staging_songs;
""")

artist_table_insert = ("""
    insert into artists \
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude \
    from staging_songs; 
""")

time_table_insert = ("""
    insert into time \
    select t.ts, \
            EXTRACT(hour from t.ts), \
            EXTRACT(day from t.ts), \
            EXTRACT(week from t.ts), \
            EXTRACT(month from t.ts), \
            EXTRACT(year from t.ts), \
            EXTRACT(dow from t.ts) \
    from ( \
        select distinct timestamp 'epoch' + ts/1000 * interval '1 second' as ts \
        from staging_events \
        where page = 'NextSong' \
        ) t;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
