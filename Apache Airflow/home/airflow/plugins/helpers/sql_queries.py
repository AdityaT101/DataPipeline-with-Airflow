class SqlQueries:
    songplay_table_insert = ("""
    insert into songplays( playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent ) 
        SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id songid, 
                songs.artist_id artistid, 
                events.sessionid, 
                events.location, 
                events.useragent user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    songplay_table_delete = ("""
    DELETE from songplays;
    """)

    
    
    user_table_insert = ("""
    insert into users( userid, first_name, last_name, gender, level ) 
      SELECT 
        distinct userid, 
        firstname first_name, 
        lastname last_name, 
        gender, 
        level
        FROM staging_events
        WHERE page='NextSong'
    """)

    
    user_table_delete = ("""
    DELETE from users;
    """)
    
    
    
    song_table_insert = ("""
    insert into songs( songid, title, artistid, year, duration ) 
      SELECT 
        distinct song_id songid, 
        title,
        artist_id artistid, 
        year,
        duration
        FROM staging_songs
    """)

    
    song_table_delete = ("""
    DELETE from songs;
    """)
    
    
    
    artist_table_insert = ("""
    insert into artists( artistid, name, location, lattitude, longitude ) 
      SELECT 
          distinct artist_id artistid, 
          artist_name name,
          artist_location,
          artist_latitude lattitude, 
          artist_longitude longitude
          FROM staging_songs
    """)

    
    
    artist_table_delete = ("""
    DELETE from artists;
    """)
    
    
    
    time_table_insert = ("""
    insert into time( start_time, hour , day , week , month , year , dayofweek ) 
        SELECT 
            start_time, 
            extract(hour from start_time) , 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplays
    """)
    
    
    time_table_delete = ("""
    DELETE from time;
    """)
    
    