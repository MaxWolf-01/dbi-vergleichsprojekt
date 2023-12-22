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
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with MongoDbContainer() as mongo:
                connection_string: str = mongo.get_connection_url()
                mongo_client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
                mongo_db: Database = mongo_client["DBIMusicPlayer"]

                if init_func:
                    init_func(mongo_db)

                def to_time():
                    test_func(mongo_db, *args, **kwargs)

                timer = Timer(to_time)
                time_taken = timer.timeit(number=n_tests)

                print(f"'{test_func.__name__}' Completed - Time taken: {time_taken:.4f} seconds")
                return time_taken

        return wrapper

    return decorator


# def init_mongo_db(mongo_db: Database) -> None:
#     collection = mongo_db.test_collection
#     # Seed the database with data
#     collection.insert_many([{"number": i} for i in range(1000)])


@mongo_performance_test()
def test_data_insertion_performance_100(mongo_db: Database) -> None:
    artists = mongo_db.artists
    albums = mongo_db.albums
    songs = mongo_db.songs
    playlists = mongo_db.playlists
    artists_albums = mongo_db.artists_albums
    songs_playlists = mongo_db.songs_playlists

    albums.insert_many([{"name": faker.catch_phrase()} for _ in range(100)])
    artists.insert_many([{
        "name": faker.name(),
        "albums": [albums.aggregate([{"$sample": {"size": 1}}]).next()['_id'] for _ in range(5)]
    } for _ in range(100)])
    songs.insert_many([{
        "title": faker.sentence(),
        "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
        "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
        "yt_link": faker.url(),
        "artist_id": artists.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "album_id": albums.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    } for _ in range(100)])
    playlists.insert_many([{
        "name": faker.catch_phrase(),
        "songs": [songs.aggregate([{"$sample": {"size": 1}}]).next()['_id'] for _ in range(5)]
    } for _ in range(100)])
    artists_albums.insert_many([{
        "artist_id": artists.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "album_id": albums.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    } for _ in range(100)])
    songs_playlists.insert_many([{
        "song_id": songs.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "playlist_id": playlists.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    } for _ in range(100)])


# @mongo_performance_test()
# def test_data_insertion_performance(mongo_db: Database) -> None:
#     collection = mongo_db.test_collection
#
#     for i in range(1000):
#         collection.insert_one({"number": i})


@mongo_performance_test(init_func=test_data_insertion_performance_100)
def test_read_performance(mongo_db: Database) -> None:
    artists = mongo_db.artists
    # Performance testing for read operations
    _ = artists.find({})


if __name__ == "__main__":
    test_data_insertion_performance_100()
    test_read_performance()
