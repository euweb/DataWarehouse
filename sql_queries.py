import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar NOT NULL,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id int,
    location varchar,
    user_agent varchar,
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar(1) NOT NULL,
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration numeric,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    format as json {}
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)
SELECT
    DATEADD(ms, ts, '1970-01-01 00:00:00') AS start_time,
    events.userId as user_id,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionId AS session_id,
    events.location,
    events.userAgent AS user_agent
FROM staging_events events
JOIN staging_songs songs
ON (events.song = songs.title
AND events.length = songs.duration
AND events.artist = songs.artist_name)
WHERE events.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    last_value(level) over (
            partition by userId
            rows between unbounded preceding and unbounded following)
FROM staging_events WHERE userId is NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(DATEADD(ms, ts, '1970-01-01 00:00:00')) as start_time,
        EXTRACT (hour FROM start_time) AS hour,
        EXTRACT (day FROM start_time) AS day,
        EXTRACT (week FROM start_time) AS week,
        EXTRACT (month FROM start_time) AS month,
        EXTRACT (year FROM start_time) AS year,
        EXTRACT (weekday FROM start_time) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, user_table_create,
                        artist_table_create, song_table_create,
                        time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, artist_table_insert,
                        song_table_insert, time_table_insert,
                        songplay_table_insert]
