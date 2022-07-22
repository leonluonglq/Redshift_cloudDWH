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
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth VARCHAR NOT NULL,
firstname VARCHAR ,
gender VARCHAR,
itemInSession INTEGER NOT NULL,
lastName VARCHAR,
length numeric(18,0),
level VARCHAR ,
location VARCHAR,
method VARCHAR ,
page VARCHAR ,
registration numeric(18,0),
sessionId INTEGER NOT NULL,
song VARCHAR,
status INTEGER ,
ts BIGINT NOT NULL,
userAgent VARCHAR,
userId INTEGER
);
""")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs INTEGER ,
artist_id VARCHAR ,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location varchar(256),
artist_name VARCHAR ,
song_id VARCHAR ,
title VARCHAR ,
duration numeric(18,0) ,
year INTEGER
);
""")
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL sortkey distkey,
user_id int NOT NULL,
level varchar NOT NULL,
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location varchar NOT NULL,
user_agent varchar NOT NULL);
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INTEGER PRIMARY KEY ,
first_name varchar NOT NULL,
last_name varchar NOT NULL,
gender char(1) NOT NULL,
level varchar NOT NULL);
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
title varchar NOT NULL,
artist_id varchar NOT NULL ,
year int NOT NULL,
duration numeric NOT NULL);
""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude numeric,
longitude numeric);
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time timestamp NOT NULL sortkey distkey PRIMARY KEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday VARCHAR);
""")
# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + (se.ts/1000)*INTERVAL '1 second' as start_time,
        se.userId as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as  location,
        se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON (ss.title=se.song AND se.artist=ss.artist_name)
    AND se.page=' NextSong';
""")
user_table_insert = ("""
INSERT INTO USERS(user_id,first_name,last_name,gender,level)
    SELECT DISTINCT userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page='NextSong'
""")
song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration 
    FROM staging_songs
""")
artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
    SELECT DISTINCT artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs
""")
time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000)*INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        TO_CHAR(start_time,'day') as weekday
    FROM staging_events
""")
# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]