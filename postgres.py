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
        def wrapper(*args: Any, init_func_n: int | None = None, **kwargs: Any) -> Any:
            with PostgresContainer("postgres:latest") as postgres:
                db_url: str = postgres.get_connection_url()
                db_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
                connection: PgConnection = psycopg2.connect(db_url)
                create_postgres_schema(connection)

                if init_func:
                    print("Applying init function")
                    init_fn_kwargs = {"n": init_func_n} if init_func_n else {}
                    init_func(connection, **init_fn_kwargs)

                def to_time():
                    test_func(connection, *args, **kwargs)

                timer = Timer(to_time)
                time_taken = timer.timeit(number=n_tests)

                print(f"'{test_func.__name__}({args, kwargs})' "
                      f"{n_tests} Tests Completed - Time taken: {time_taken:.4f} seconds")

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


def insert_fake_data(connection: PgConnection, n: int) -> None:
    with connection.cursor() as cursor:
        for _ in range(n):
            cursor.execute("INSERT INTO A_Artists (A_Name) VALUES (%s)", (faker.name(),))
            cursor.execute("INSERT INTO Al_Albums (Al_Name) VALUES (%s)", (faker.catch_phrase(),))
            cursor.execute("INSERT INTO P_Playlists (P_Name) VALUES (%s)", (faker.catch_phrase(),))
            song_title = faker.sentence()
            song_length = round(faker.pydecimal(left_digits=2, right_digits=2, positive=True), 2)
            song_rating = round(faker.pydecimal(left_digits=1, right_digits=1, positive=True), 1)
            yt_link = faker.url()
            cursor.execute("""INSERT INTO S_Songs (S_Title, S_Length, S_Rating, S_YT_Link, S_Al_ID) 
                VALUES (%s, %s, %s, %s, (SELECT Al_ID FROM Al_Albums ORDER BY RANDOM() LIMIT 1))
                """, (song_title, song_length, song_rating, yt_link))
            cursor.execute("""INSERT INTO Al_Albums_have_A_Artists (Al_ID, A_ID)
                VALUES ((SELECT Al_ID FROM Al_Albums ORDER BY RANDOM() LIMIT 1), (SELECT A_ID FROM A_Artists ORDER BY RANDOM() LIMIT 1))
                """)
            cursor.execute("""INSERT INTO P_Playlists_have_S_Songs (P_ID, S_ID)
                VALUES ((SELECT P_ID FROM P_Playlists ORDER BY RANDOM() LIMIT 1), (SELECT S_ID FROM S_Songs ORDER BY RANDOM() LIMIT 1))
                """)
    connection.commit()


def insert_many_fake_data(connection: PgConnection, n: int) -> None:
    cursor = connection.cursor()

    artists_data = [(faker.name(),) for _ in range(n)]
    albums_data = [(faker.word(),) for _ in range(n)]
    playlists_data = [(faker.word(),) for _ in range(n)]
    songs_data = [
        (faker.sentence(), faker.random_number(digits=2), faker.random_number(digits=1), faker.url(), i + 1)
        for i in range(n)
    ]
    album_artist_data = [(i + 1, i + 1) for i in range(n)]
    playlist_song_data = [(i + 1, i + 1) for i in range(n)]

    cursor.executemany("INSERT INTO A_Artists (A_Name) VALUES (%s);", artists_data)
    cursor.executemany("INSERT INTO Al_Albums (Al_Name) VALUES (%s);", albums_data)
    cursor.executemany("INSERT INTO Al_Albums_have_A_Artists (Al_ID, A_ID) VALUES (%s, %s);", album_artist_data)
    cursor.executemany("INSERT INTO P_Playlists (P_Name) VALUES (%s);", playlists_data)
    cursor.executemany(
        "INSERT INTO S_Songs (S_Title, S_Length, S_Rating, S_YT_Link, S_Al_ID) VALUES (%s, %s, %s, %s, %s);",
        songs_data)
    cursor.executemany("INSERT INTO P_Playlists_have_S_Songs (P_ID, S_ID) VALUES (%s, %s);", playlist_song_data)

    connection.commit()
    cursor.close()


@postgres_performance_test(init_func=insert_many_fake_data)
def test_read_performance(connection: PgConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM SongsInAPlaylist")
        _ = cursor.fetchall()


@postgres_performance_test()
def test_insert_performance(connection: PgConnection, n: int) -> None:
    insert_fake_data(connection, n)


@postgres_performance_test()
def test_insert_many_performance(connection: PgConnection, n: int) -> None:
    insert_many_fake_data(connection, n)


@postgres_performance_test(init_func=insert_many_fake_data)
def test_delete_performance(connection: PgConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM P_Playlists_have_S_Songs;")
        cursor.execute("DELETE FROM S_Songs;")
        cursor.execute("DELETE FROM Al_Albums_have_A_Artists;")
        cursor.execute("DELETE FROM P_Playlists;")
        cursor.execute("DELETE FROM Al_Albums;")
        cursor.execute("DELETE FROM A_Artists;")
    connection.commit()


@postgres_performance_test(init_func=insert_many_fake_data)
def test_update_performance(connection: PgConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("UPDATE A_Artists SET A_Name = 'Updated Artist Name';")
        cursor.execute("UPDATE S_Songs SET S_Length = 3.50;")
    connection.commit()
