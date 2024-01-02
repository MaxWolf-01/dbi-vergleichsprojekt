from functools import wraps
from timeit import Timer
from typing import Any, Callable, Optional

import pymongo
from faker import Faker
from pymongo.database import Database
from testcontainers.mongodb import MongoDbContainer
from bson.decimal128 import Decimal128

faker: Faker = Faker()


def mongo_performance_test(init_func: Optional[Callable] = None, n_tests: int = 10) -> Callable:
    def decorator(test_func: Callable) -> Callable:
        @wraps(test_func)
        def wrapper(*args: Any, init_func_n: int | None = None, **kwargs: Any) -> Any:
            with MongoDbContainer() as mongo:
                connection_string: str = mongo.get_connection_url()
                mongo_client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
                mongo_db: Database = mongo_client["DBIMusicPlayer"]

                if init_func:
                    print("Applying init function")
                    init_fn_kwargs = {"n": init_func_n} if init_func_n else {}
                    init_func(**init_fn_kwargs)

                def to_time():
                    test_func(mongo_db, *args, **kwargs)

                timer = Timer(to_time)
                time_taken = timer.timeit(number=n_tests)

                print(f"'{test_func.__name__}({args, kwargs})' "
                      f"{n_tests} Tests Completed - Time taken: {time_taken:.4f} seconds")

                return time_taken

        return wrapper

    return decorator


def init_mongo_db(mongo_db: Database) -> None:
    collection = mongo_db.test_collection
    # Seed the database with data
    collection.insert_many([{"number": i} for i in range(1000)])


def insert_fake_data(mongo_db: Database, n: int) -> None:
    for _ in range(n):
        artist_result = mongo_db.artists.insert_one({"name": faker.name()})
        artist_id = artist_result.inserted_id
        album_result = mongo_db.albums.insert_one({"name": faker.word()})
        album_id = album_result.inserted_id
        mongo_db.songs.insert_one({
            "title": faker.sentence(),
            "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
            "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
            "yt_link": faker.url(),
            "artist_id": artist_id,
            "album_id": album_id
        })
        playlist_result = mongo_db.playlists.insert_one({"name": faker.catch_phrase()})
        playlist_id = playlist_result.inserted_id
        mongo_db.artists_albums.insert_one({
            "artist_id": artist_id,
            "album_id": album_id
        })
        song_id = mongo_db.songs.find_one(sort=[('_id', -1)])['_id']
        mongo_db.songs_playlists.insert_one({
            "song_id": song_id,
            "playlist_id": playlist_id
        })


def insert_many_fake_data(mongo_db: Database, n: int) -> None:
    artists_data = [{"name": faker.name()} for _ in range(n)]
    albums_data = [{"name": faker.word()} for _ in range(n)]
    playlists_data = [{"name": faker.word()} for _ in range(n)]
    mongo_db.artists.insert_many(artists_data)
    mongo_db.albums.insert_many(albums_data)
    mongo_db.playlists.insert_many(playlists_data)

    artist_id = mongo_db.artists.find_one(sort=[('_id', -1)])['_id']
    album_id = mongo_db.albums.find_one(sort=[('_id', -1)])['_id']
    songs_data = [
        {
            "title": faker.sentence(),
            "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
            "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
            "yt_link": faker.url(),
            "artist_id": artist_id,
            "album_id": album_id,
        }
        for _ in range(n)
    ]
    mongo_db.songs.insert_many(songs_data)

    album_artist_data = [
        {
            "artist_id": artist_id,
            "album_id": album_id,
        }
        for _ in range(n)
    ]
    mongo_db.artists_albums.insert_many(album_artist_data)

    song_id = mongo_db.songs.find_one(sort=[('_id', -1)])['_id']
    playlist_id = mongo_db.playlists.find_one(sort=[('_id', -1)])['_id']
    playlist_song_data = [
        {
            "song_id": song_id,
            "playlist_id": playlist_id,
        }
        for _ in range(n)
    ]
    mongo_db.songs_playlists.insert_many(playlist_song_data)


@mongo_performance_test()
def test_insert_performance(mongo_db: Database, n: int) -> None:
    insert_fake_data(mongo_db, n)


@mongo_performance_test()
def test_insert_many_performance(mongo_db: Database, n: int) -> None:
    insert_many_fake_data(mongo_db, n)


@mongo_performance_test(init_func=insert_many_fake_data)
def test_read_performance(mongo_db: Database) -> None:
    ...
