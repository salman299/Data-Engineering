import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_events(
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      BIGINT,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_songs
    (
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT4,
        year                SMALLINT,
        artist_id           TEXT,
        artist_name         TEXT,
        artist_latitude     FLOAT4,
        artist_longitude    FLOAT4,
        artist_location     TEXT,
        num_songs           INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id     BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time      TIMESTAMP NOT NULL SORTKEY,
        user_id         TEXT NOT NULL DISTKEY,
        level           TEXT NOT NULL,
        song_id         TEXT,
        artist_id       TEXT,
        session_id      BIGINT NOT NULL,
        location        TEXT NOT NULL,
        user_agent      TEXT NOT NULL
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id      TEXT PRIMARY KEY DISTKEY,
        first_name   TEXT NOT NULL,
        last_name    TEXT NOT NULL,
        gender       CHAR(1) NOT NULL,
        level        TEXT NOT NULL
    ) diststyle key;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id     TEXT PRIMARY KEY SORTKEY,
        title       TEXT NOT NULL,
        artist_id   TEXT NOT NULL,
        year        SMALLINT NOT NULL,
        duration    FLOAT4 NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id   TEXT PRIMARY KEY SORTKEY,
        name        TEXT NOT NULL,
        location    TEXT NOT NULL,
        latitude    FLOAT4,
        longitude   FLOAT4
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT NOT NULL,
        day         SMALLINT NOT NULL,
        week        SMALLINT NOT NULL,
        month       SMALLINT NOT NULL,
        year        SMALLINT NOT NULL,
        weekday     SMALLINT NOT NULL
    ) diststyle all;
""")

# STAGING TABLES
REGION='us-west-2'
staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'stage_events',
    config['S3']['LOG_DATA'], 
    config['IAM_ROLE']['ARN'], 
    config['S3']['LOG_JSONPATH'], 
    REGION)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format('stage_songs',
            config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
                se.user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.location,
                se.user_agent
FROM stage_songs ss
INNER JOIN stage_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
FROM stage_events
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
FROM stage_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM stage_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH date_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as start_time FROM stage_events)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM date_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
