class SqlQueries:

    songplay_table_insert = ("""
        insert into public.songplays
        SELECT md5(events.sessionid || events.start_time),
               events.start_time, 
               events.userid, 
               events.level, 
               songs.song_id, 
               songs.artist_id, 
               events.sessionid, 
               events.location, 
               events.useragent
        FROM (
               SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
               FROM staging_events
               WHERE page='NextSong'
                    and ts is not null
                    and userid is not null
               ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        insert into public.users
        SELECT distinct userid, 
               firstname, 
               lastname, 
               gender, 
               level
        FROM staging_events
        WHERE page='NextSong'
            and userid is not null
    """)

    song_table_insert = ("""
        insert into public.songs
        SELECT distinct song_id, 
               title, 
               artist_id, 
               year, 
               duration
        FROM staging_songs
        where song_id is not null
    """)

    artist_table_insert = ("""
        insert into public.artists
        SELECT distinct artist_id, 
               artist_name, 
               artist_location, 
               artist_latitude, 
               artist_longitude
        FROM staging_songs
        where artist_id is not null
    """)

    time_table_insert = ("""
        insert into public.time
        SELECT start_time, 
               extract(hour from start_time), 
               extract(day from start_time), 
               extract(week from start_time), 
               extract(month from start_time), 
               extract(year from start_time), 
               extract(dayofweek from start_time)
        FROM songplays
    """)
