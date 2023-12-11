import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR (255),
    auth VARCHAR (100),
    first_name VARCHAR (50),
    gender VARCHAR (1),
    item_in_session INT,
    last_name VARCHAR (50),
    length FLOAT,
    level VARCHAR (50),
    location VARCHAR (255),
    method VARCHAR (10),
    page VARCHAR (50),
    registration FLOAT,
    session_id INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    user_agent VARCHAR(255),
    user_id INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR(50),
    artist_latitude FLOAT,
    artist_location VARCHAR(255),
    artist_longitude FLOAT,
    artist_name VARCHAR(255),
    duration FLOAT,
    num_songs INT,
    song_id VARCHAR(50),
    title VARCHAR(255),
    year SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR(50),
    song_id VARCHAR(50),
    artist_id VARCHAR(50),
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR (50),
    last_name VARCHAR (50),
    gender VARCHAR (1),
    level VARCHAR (50)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(50),
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                            iam_role {}
                            format as json {}
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""COPY staging_songs FROM {}
                            iam_role {}
                            format as json 'auto'
""").format(config['S3']['song_data'], config['IAM_ROLE']['arn'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  
    TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' as start_time,
    events.user_id, 
    events.level, 
    songs.song_id,
    songs.artist_id, 
    events.session_id,
    events.location, 
    events.user_agent
FROM staging_events events
LEFT JOIN staging_songs songs
ON events.song = songs.title AND
   events.artist = songs.artist_name
WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT  
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
""")


artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT  
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT DISTINCT 
    start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
