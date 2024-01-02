from faker import Faker
from pymongo import MongoClient
faker: Faker = Faker()

client = MongoClient('localhost', 27017, username='root', password='1234')
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
