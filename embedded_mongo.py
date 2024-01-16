import random
from bson.json_util import dumps
import pymongo
from faker import Faker
from testcontainers.mongodb import MongoDbContainer
from bson.decimal128 import Decimal128

faker: Faker = Faker()

mongo = MongoDbContainer()
mongo.start()
connection_string = mongo.get_connection_url()
mongo_client = pymongo.MongoClient(connection_string)
db = mongo_client["DBIMusicPlayer"]

# %%
for _ in range(100):
    is_playlist = random.random() < 0.7
    artists = [] if is_playlist else [faker.name() for _ in range(random.randint(1, 3))]

    playlist_doc = {
        "name": faker.catch_phrase(),
        "artists": artists if artists else None
    }
    playlist_id = db.playlists.insert_one(playlist_doc).inserted_id

    song_doc = {
        "title": faker.sentence(),
        "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
        "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
        "yt_link": faker.url(),
        "artists": [faker.name() for _ in range(random.randint(1, 3))],
        "playlists": [playlist_id]
    }
    song_id = db.songs.insert_one(song_doc).inserted_id

    db.playlists.update_one({"_id": playlist_id}, {"$push": {"songs": song_id}})

# %%
print('First 5 Songs:\n')
for song in db.songs.find().limit(5):
    print(song)
print('\nFirst 5 Playlists:\n')
for playlist in db.playlists.find().limit(5):
    print(playlist)

# %%
pipeline = [
    {
        '$lookup': {
            'from': 'playlists',
            'localField': 'playlists',
            'foreignField': '_id',
            'as': 'playlist_info'
        }
    },
    {
        '$unwind': '$playlist_info'
    },
    {
        '$project': {
            'playlist_id': '$playlist_info._id',
            'playlist_name': '$playlist_info.name',
            'song_id': '$_id',
            'song_title': '$title',
            'song_length': '$length',
            'song_rating': '$rating',
            'song_yt_link': '$yt_link',
            'artist_names': '$artists',
            'album_name': {
                '$cond': {'if': {'$isArray': '$playlist_info.artists'}, 'then': '$playlist_info.name', 'else': ''}}
        }
    },
    {
        '$limit': 5
    }
]

results = db.songs.aggregate(pipeline)
for result in results:
    print(dumps(result, indent=4))

# %%

print("\nAdd song to playlist\n---------------\n")
playlist = db.playlists.find_one()
print("BEFORE:\n", playlist)
song = db.songs.find_one()
db.playlists.update_one({"_id": playlist["_id"]}, {"$push": {"songs": song["_id"]}})
playlist = db.playlists.find_one({"_id": playlist["_id"]})
print("AFTER:\n", playlist)

# %%


print("\nDelete song from playlist\n---------------\n")
playlist = db.playlists.find_one()
song = db.songs.find_one()
print("Playlist BEFORE deletion:\n", playlist)
playlist_id = playlist['_id']
song_id = song['_id']
db.playlists.update_one({"_id": playlist_id}, {"$pull": {"songs": song_id}})
playlist_after_deletion = db.playlists.find_one({"_id": playlist_id})
print("Playlist AFTER deletion:\n", playlist_after_deletion)
