class SqlQueries:
    # TABLE NAME
    STAGING_EVENTS = "staging_events"
    STAGING_SONGS = "staging_songs"

    SONG_PLAY = "songplays"
    USERS = "users"
    SONGS = "songs"
    ARTISTS = "artists"
    TIME = "time"
    # SQL FORMAT
    drop_table = "DROP TABLE IF EXISTS {table_name};"
    create_table = "CREATE TABLE IF NOT EXISTS {table_name} ({atbs});"
    create_primary_key = "PRIMARY KEY ({primary_keys})"
    create_foreign_key = "FOREIGN KEY ({foreign_key}) references {table_references}({col_references})"
    insert = "INSERT INTO {table_name} ({atbs}) VALUES ({values})"
    insert_from_select = "INSERT INTO {des_tbl} ({atbs}) SELECT {values} FROM {org_tbl};"
    select = "SELECT {atbs} FROM {table_name} {where} {order};"
    create_index = "CREATE INDEX {index_name} ON {table_name} ({col})"
    where = "WHERE {condition}"
    order = "ORDER BY {order}"

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = insert_from_select.format(
        des_tbl=USERS,
        atbs="userid, first_name, last_name, gender, level",
        values="""
        DISTINCT
            userid         ,
            firstName       AS first_name,
            lastName        AS last_name,
            gender,
            level  
        """,
        org_tbl=STAGING_EVENTS + " where userid is not null and page='NextSong'",
    )

    song_table_insert = insert_from_select.format(
        des_tbl=SONGS,
        atbs="songid, title, artistid, year, duration",
        values="""
        DISTINCT
            song_id     as songid,
            title,
            artist_id   as artistid,
            year,
            duration
        """,
        org_tbl=STAGING_SONGS,
    )

    artist_table_insert = insert_from_select.format(
        des_tbl=ARTISTS,
        atbs="artistid, name, location, lattitude, longitude",
        values="""
        DISTINCT
            artist_id           AS artistid,
            artist_name         AS name,
            artist_location     AS "location",
            artist_latitude     AS lattitude,
            artist_longitude    AS longitude
        """,
        org_tbl=STAGING_SONGS,
    )

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    validate_user = """
        select count(*) from users where first_name is null
    """
