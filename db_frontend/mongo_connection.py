from bson import Decimal128
from faker import Faker
from pymongo import MongoClient
faker: Faker = Faker()
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://root:1234@dbivergleichsprojekt.ipzeaff.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# client = MongoClient('localhost', 27017, username='root', password='1234')
# client = MongoClient('mongodb+srv://root:1234@dbimusicplayer.b6ocnvr.mongodb.net/?retryWrites=true&w=majority')
db = client['KeanuBeats']

db.drop_collection('artists')
db.drop_collection('albums')
db.drop_collection('songs')
db.drop_collection('playlists')
db.drop_collection('albumsHaveArtists')
db.drop_collection('playlistsHaveSongs')

artists = db.create_collection('artists')
albums = db.create_collection('albums')
songs = db.create_collection('songs')
playlists = db.create_collection('playlists')
albumsHaveArtists = db.create_collection('albumsHaveArtists')
playlistsHaveSongs = db.create_collection('playlistsHaveSongs')

for i in range(100):
    artist = {"name": faker.name()}
    artists.insert_one(artist)

    album = {"name": faker.catch_phrase()}
    albums.insert_one(album)

    song = {
        "title": faker.sentence(),
        "length": Decimal128(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),
        "rating": Decimal128(faker.pydecimal(left_digits=1, right_digits=1, positive=True)),
        "yt_link": faker.url(),
        "artist_id": artists.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "album_id": albums.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    }
    songs.insert_one(song)

    playlist = {
        "name": faker.catch_phrase(),
        "songs": [songs.aggregate([{"$sample": {"size": 1}}]).next()['_id'] for _ in range(5)] # check if what this doing is correct: new ObjectId()?????
    }
    playlists.insert_one(playlist)

    albumHasArtist = {
        "artist_id": artists.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "album_id": albums.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    }
    albumsHaveArtists.insert_one(albumHasArtist)

    playlistHasSong = {
        "song_id": songs.aggregate([{"$sample": {"size": 1}}]).next()['_id'],
        "playlist_id": playlists.aggregate([{"$sample": {"size": 1}}]).next()['_id']
    }
    playlistsHaveSongs.insert_one(playlistHasSong)
