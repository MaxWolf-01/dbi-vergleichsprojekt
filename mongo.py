import functools
from contextlib import contextmanager
from typing import Callable, Optional

import pymongo
from faker import Faker
from pymongo.database import Database
from testcontainers.mongodb import MongoDbContainer
from bson.decimal128 import Decimal128

from performance_test import measure_performance

faker: Faker = Faker()


def mongo_performance_test(init_func: Optional[Callable] = None):
    @contextmanager
    def mongo_context():
        mongo = MongoDbContainer()
        mongo.start()
        connection_string = mongo.get_connection_url()
        mongo_client = pymongo.MongoClient(connection_string)
        db = mongo_client["DBIMusicPlayer"]  # Adjust the database name as needed
        try:
            yield db
        finally:
            mongo_client.close()
            mongo.stop()

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, n_tests: int = 10, **kwargs):
            with mongo_context() as db:
                return measure_performance(db=db, test_func=func, init_func=init_func, n_tests=n_tests, *args, **kwargs)

        return wrapper

    return decorator


def init_mongo_db(mongo_db: Database) -> None:
    collection = mongo_db.test_collection
    # Seed the database with data
    collection.insert_many([{"number": i} for i in range(1000)])


def insert_fake_data(mongo_db: Database, n: int) -> None:
    for _ in range(n):
        artist_id = mongo_db.artists.insert_one({"name": faker.name()}).inserted_id
        album_id = mongo_db.albums.insert_one({"name": faker.word()}).inserted_id
        song_id = mongo_db.songs.insert_one({
            "title": faker.sentence(),
            "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
            "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
            "yt_link": faker.url(),
            "artist_id": artist_id,
            "album_id": album_id
        }).inserted_id
        playlist_id = mongo_db.playlists.insert_one({"name": faker.catch_phrase()}).inserted_id
        mongo_db.artists_albums.insert_one({
            "artist_id": artist_id,
            "album_id": album_id
        })
        mongo_db.songs_playlists.insert_one({
            "song_id": song_id,
            "playlist_id": playlist_id
        })


def insert_many_fake_data(mongo_db: Database, n: int) -> None:
    artists_data = [{"name": faker.name()} for _ in range(n)]
    albums_data = [{"name": faker.word()} for _ in range(n)]
    playlists_data = [{"name": faker.word()} for _ in range(n)]
    artists = mongo_db.artists.insert_many(artists_data).inserted_ids
    albums = mongo_db.albums.insert_many(albums_data).inserted_ids
    playlists = mongo_db.playlists.insert_many(playlists_data).inserted_ids
    songs_data = [
        {
            "title": faker.sentence(),
            "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
            "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
            "yt_link": faker.url(),
            "artist_id": artists[i % n],
            "album_id": albums[i % n],
        }
        for i in range(n)
    ]
    songs = mongo_db.songs.insert_many(songs_data).inserted_ids
    album_artist_data = [{"artist_id": artist_id, "album_id": album_id} for artist_id, album_id in zip(artists, albums)]
    playlist_song_data = [
        {"song_id": song_id, "playlist_id": playlist_id} for song_id, playlist_id in zip(songs, playlists)
    ]
    mongo_db.artists_albums.insert_many(album_artist_data)
    mongo_db.songs_playlists.insert_many(playlist_song_data)


@mongo_performance_test()
def test_insert_performance(mongo_db: Database, n: int) -> None:
    insert_fake_data(mongo_db, n)


@mongo_performance_test()
def test_insert_many_performance(mongo_db: Database, n: int) -> None:
    insert_many_fake_data(mongo_db, n)


@mongo_performance_test(init_func=insert_many_fake_data)
def test_read_performance(mongo_db: Database) -> None:
    pipeline = [
        {
            "$lookup": {
                "from": "songs_playlists",
                "localField": "_id",
                "foreignField": "song_id",
                "as": "playlist_info"
            }
        },
        {
            "$unwind": "$playlist_info"
        },
        {
            "$lookup": {
                "from": "playlists",
                "localField": "playlist_info.playlist_id",
                "foreignField": "_id",
                "as": "playlist"
            }
        },
        {
            "$unwind": "$playlist"
        },
        {
            "$lookup": {
                "from": "artists_albums",
                "localField": "album_id",
                "foreignField": "album_id",
                "as": "artist_album"
            }
        },
        {
            "$unwind": "$artist_album"
        },
        {
            "$lookup": {
                "from": "artists",
                "localField": "artist_album.artist_id",
                "foreignField": "_id",
                "as": "artist"
            }
        },
        {
            "$unwind": "$artist"
        },
        {
            "$project": {
                "playlist_id": "$playlist._id",
                "playlist_name": "$playlist.name",
                "song_id": "$_id",
                "song_title": "$title",
                "song_length": "$length",
                "song_rating": "$rating",
                "yt_link": "$yt_link",
                "artist_id": "$artist._id",
                "artist_name": "$artist.name",
                "album_id": "$album_id",
            }
        }
    ]
    songs_in_playlist = mongo_db.songs.aggregate(pipeline)
    _ = list(songs_in_playlist)


@mongo_performance_test(init_func=insert_many_fake_data)
def test_delete_performance(mongo_db: Database) -> None:
    mongo_db.artists.delete_many({})
    mongo_db.albums.delete_many({})
    mongo_db.playlists.delete_many({})
    mongo_db.songs.delete_many({})
    mongo_db.artists_albums.delete_many({})
    mongo_db.songs_playlists.delete_many({})


@mongo_performance_test(init_func=insert_many_fake_data)
def test_update_performance(mongo_db: Database) -> None:
    mongo_db.artists.update_many({}, {"$set": {"name": "Updated Artist Name"}})
    mongo_db.songs.update_many({}, {"$set": {"length": Decimal128("3.50")}})
