from functools import wraps
from timeit import Timer
from typing import Any, Callable, Optional

import psycopg2
from faker import Faker
from psycopg2.extensions import connection as PgConnection
from testcontainers.postgres import PostgresContainer

faker: Faker = Faker()


def postgres_performance_test(init_func: Optional[Callable] = None, n_tests: int = 10) -> Callable:
    def decorator(test_func: Callable) -> Callable:
        @wraps(test_func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with PostgresContainer("postgres:latest") as postgres:
                db_url: str = postgres.get_connection_url()
                db_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
                connection: PgConnection = psycopg2.connect(db_url)
                create_postgres_schema(connection)

                if init_func:
                    init_func(connection)

                def to_time():
                    test_func(connection, *args, **kwargs)

                timer = Timer(to_time)
                time_taken = timer.timeit(number=n_tests)

                print(f"'{test_func.__name__}' Completed - Time taken: {time_taken:.4f} seconds")

                connection.close()
                return time_taken

        return wrapper

    return decorator


def create_postgres_schema(conection: PgConnection) -> None:
    with conection.cursor() as cursor:
        cursor.execute("""
          CREATE TABLE A_Artists (
            A_ID SERIAL PRIMARY KEY,
            A_Name VARCHAR
        );

        CREATE TABLE Al_Albums (
            Al_ID SERIAL PRIMARY KEY,
            Al_Name VARCHAR
        );

        CREATE TABLE Al_Albums_have_A_Artists (
            Al_A_ID SERIAL PRIMARY KEY,
            Al_ID INTEGER REFERENCES Al_Albums(Al_ID),
            A_ID INTEGER REFERENCES A_Artists(A_ID)
        );

        CREATE TABLE P_Playlists (
            P_ID SERIAL PRIMARY KEY,
            P_Name VARCHAR
        );

        CREATE TABLE S_Songs (
            S_ID SERIAL PRIMARY KEY,
            S_Title VARCHAR,
            S_Length DECIMAL(5,2),
            S_Rating DECIMAL(2,1),
            S_YT_Link VARCHAR,
            S_Al_ID INTEGER REFERENCES Al_Albums(Al_ID)
        );

        CREATE TABLE P_Playlists_have_S_Songs (
            P_S_ID SERIAL PRIMARY KEY,
            P_ID INTEGER REFERENCES P_Playlists(P_ID),
            S_ID INTEGER REFERENCES S_Songs(S_ID)
        );

        -- View for Songs in a Playlist
        CREATE VIEW SongsInAPlaylist AS
        SELECT
            P_Playlists.P_Id,
            P_Playlists.P_Name,
            S_Songs.S_Id,
            S_Songs.S_Title,
            S_Songs.S_Length,
            S_Songs.S_Rating,
            S_Songs.S_YT_Link,
            A_Artists.A_Id,
            A_Artists.A_Name,
            Al_Albums.Al_Id,
            Al_Albums.Al_Name
        FROM
            P_Playlists
            JOIN P_Playlists_have_S_Songs ON P_Playlists.P_Id = P_Playlists_have_S_Songs.P_Id
            JOIN S_Songs ON P_Playlists_have_S_Songs.S_Id = S_Songs.S_Id
            LEFT JOIN Al_Albums ON S_Songs.S_Al_ID = Al_Albums.Al_Id
            LEFT JOIN Al_Albums_have_A_Artists ON Al_Albums.Al_Id = Al_Albums_have_A_Artists.Al_Id
            LEFT JOIN A_Artists ON Al_Albums_have_A_Artists.A_Id = A_Artists.A_Id
        ORDER BY
        P_Playlists.P_Id, S_Songs.S_Id;
        """)
    conection.commit()


def insert_fake_data_postgres(connection: PgConnection, num_entries: int = 100) -> None:
    with connection.cursor() as cursor:
        for _ in range(num_entries):
            # Inserting Artists
            artist_name = faker.name()
            cursor.execute("INSERT INTO A_Artists (A_Name) VALUES (%s)", (artist_name,))

            # Inserting Albums
            album_name = faker.catch_phrase()
            cursor.execute("INSERT INTO Al_Albums (Al_Name) VALUES (%s)", (album_name,))

            # Inserting Playlists
            playlist_name = faker.catch_phrase()
            cursor.execute("INSERT INTO P_Playlists (P_Name) VALUES (%s)", (playlist_name,))

            # Inserting Songs
            song_title = faker.sentence()
            song_title = faker.sentence()
            song_length = round(faker.pydecimal(left_digits=2, right_digits=2, positive=True), 2)
            song_rating = round(faker.pydecimal(left_digits=1, right_digits=1, positive=True), 1)
            yt_link = faker.url()
            cursor.execute("""INSERT INTO S_Songs (S_Title, S_Length, S_Rating, S_YT_Link, S_Al_ID) 
                VALUES (%s, %s, %s, %s, (SELECT Al_ID FROM Al_Albums ORDER BY RANDOM() LIMIT 1))
                """, (song_title, song_length, song_rating, yt_link))
    connection.commit()


@postgres_performance_test()
def test_data_insertion_performance(connection: PgConnection) -> None:
    ...


@postgres_performance_test(init_func=insert_fake_data_postgres)
def test_read_performance(connection: PgConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM SongsInAPlaylist")
        _ = cursor.fetchall()


@postgres_performance_test()
def test_create_performance(connection: PgConnection, n: int = 100) -> None:
    cursor = connection.cursor()
    for _ in range(n):  # Adjust for scalability
        cursor.execute("INSERT INTO A_Artists (A_Name) VALUES (%s) RETURNING A_ID;", (faker.name(),))
        artist_id = cursor.fetchone()[0]

        cursor.execute("INSERT INTO Al_Albums (Al_Name) VALUES (%s) RETURNING Al_ID;", (faker.word(),))
        album_id = cursor.fetchone()[0]

        cursor.execute("INSERT INTO Al_Albums_have_A_Artists (Al_ID, A_ID) VALUES (%s, %s);", (album_id, artist_id))

        cursor.execute("INSERT INTO P_Playlists (P_Name) VALUES (%s) RETURNING P_ID;", (faker.word(),))
        playlist_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO S_Songs (S_Title, S_Length, S_Rating, S_YT_Link, S_Al_ID) 
            VALUES (%s, %s, %s, %s, %s) RETURNING S_ID;
            """, (
        faker.sentence(), faker.random_number(digits=2), faker.random_number(digits=1), faker.url(), album_id))
        song_id = cursor.fetchone()[0]

        cursor.execute("INSERT INTO P_Playlists_have_S_Songs (P_ID, S_ID) VALUES (%s, %s);", (playlist_id, song_id))
    connection.commit()
    cursor.close()


if __name__ == "__main__":
    test_read_performance()
    test_create_performance()
    test_create_performance(n=1000)
