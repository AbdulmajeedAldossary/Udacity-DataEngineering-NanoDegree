import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE events_stage (
    artist_id VARCHAR ENCODE ZSTD,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session VARCHAR,
    last_name VARCHAR,
    length FLOAT8,
    level VARCHAR,
    location VARCHAR ENCODE ZSTD,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    session_id VARCHAR,
    song_title VARCHAR ENCODE ZSTD,
    status VARCHAR,
    ts VARCHAR,
    user_agent VARCHAR ENCODE ZSTD,
    user_id VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE songs_stage (
    num_songs INT,
    artist_id VARCHAR ENCODE ZSTD,
    artist_latitude FLOAT8,
    artist_longitude FLOAT8,
    artist_location VARCHAR ENCODE ZSTD,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR ENCODE ZSTD,
    duration FLOAT8,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR ENCODE ZSTD,
    session_id VARCHAR,
    location VARCHAR ENCODE ZSTD,
    user_agent VARCHAR ENCODE ZSTD
)
DISTSTYLE KEY DISTKEY (user_id)
SORTKEY(start_time);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
)
DISTSTYLE KEY DISTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR ENCODE ZSTD,
    artist_id VARCHAR ENCODE ZSTD,
    year INT,
    duration FLOAT8
)
DISTSTYLE AUTO
COMPOUND SORTKEY (title, year);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR ENCODE ZSTD,
    latitude FLOAT8,
    longitude FLOAT8
)
DISTSTYLE AUTO
SORTKEY (name);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY events_stage
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {} REGION 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
)

staging_songs_copy = ("""
COPY songs_stage
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS;
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) SELECT DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
         e.user_id as user_id,
         e.level as level,
         s.song_id as song_id,
         e.artist_id as artist_id,
         e.session_id as session_id,
         e.location as location,
         e.user_agent as user_agent
  FROM events_stage AS e
  LEFT JOIN songs_stage AS s
  ON s.artist_id = e.artist_id
  AND s.title = e.song_title
  WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM events_stage
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM songs_stage;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
       artist_name as name,
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
FROM songs_stage;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH time_parse AS (
SELECT
    DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000
    * INTERVAL '1 second' AS start_time
    FROM events_stage
)
SELECT DISTINCT start_time AS start_time,
       EXTRACT(hour FROM start_time) AS hour,
       EXTRACT(day FROM start_time) AS day,
       EXTRACT(week FROM start_time) AS week,
       EXTRACT(month FROM start_time) AS month,
       EXTRACT(year FROM start_time) AS year,
       EXTRACT(weekday FROM start_time) AS weekday
FROM time_parse;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
