from bson import ObjectId
from flask import Flask, render_template, request, redirect, url_for
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient('localhost', 27017, username='root', password='1234')
db = client['KeanuBeats']
artists = db['artists']
albums = db['albums']
songs = db['songs']
playlists = db['playlists']


def view_query():
    pipeline = [
        {
            "$lookup": {
                "from": "songs",
                "localField": "songs",
                "foreignField": "_id",
                "as": "songs_info"
            }
        },
        {
            "$unwind": "$songs_info"
        },
        {
            "$lookup": {
                "from": "artists",
                "localField": "songs_info.artist_id",
                "foreignField": "_id",
                "as": "artist_info"
            }
        },
        {
            "$unwind": "$artist_info"
        },
        {
            "$lookup": {
                "from": "albums",
                "localField": "songs_info.album_id",
                "foreignField": "_id",
                "as": "album_info"
            }
        },
        {
            "$unwind": "$album_info"
        },
        {
            "$project": {
                "playlists_id": "$_id",
                "playlists_name": "$name",
                "songs_id": "$songs_info._id",
                "songs_title": "$songs_info.title",
                "songs_length": "$songs_info.length",
                "songs_rating": "$songs_info.rating",
                "songs_yt_link": "$songs_info.yt_link",
                "artists_id": "$artist_info._id",
                "artists_name": "$artist_info.name",
                "albums_id": "$album_info._id",
                "albums_name": "$album_info.name"
            }
        }
    ]
    result = list(playlists.aggregate(pipeline))
    return result


def add_row_numbers(result):
    for i, row in enumerate(result):
        row['row_number'] = i + 1
    return result


@app.route('/')
def index():
    query_result = view_query()
    artists_result = list(artists.find())
    albums_result = list(albums.find())
    songs_result = list(songs.find())
    playlists_result = list(playlists.find())

    query_result = add_row_numbers(query_result)
    artists_result = add_row_numbers(artists_result)
    albums_result = add_row_numbers(albums_result)
    songs_result = add_row_numbers(songs_result)
    playlists_result = add_row_numbers(playlists_result)

    return render_template('index.html', query_result=query_result, artists_result=artists_result, albums_result=albums_result, songs_result=songs_result, playlists_result=playlists_result)


@app.route('/add_document', methods=['POST'])
def add_document():
    collection_name = request.form['collection_name']

    if collection_name not in ['artists', 'albums', 'songs', 'playlists']:
        return redirect(url_for('index'))

    new_document = {}

    if collection_name == 'artists':
        new_document['name'] = request.form['artist_name']
    elif collection_name == 'albums':
        new_document['name'] = request.form['album_name']
    elif collection_name == 'songs':
        new_document['title'] = request.form['song_title']
        new_document['length'] = request.form['song_length']
        new_document['rating'] = request.form['song_rating']
        new_document['yt_link'] = request.form['song_yt_link']
        new_document['artist_id'] = request.form['song_artist_id']
        new_document['album_id'] = request.form['song_album_id']
    elif collection_name == 'playlists':
        new_document['name'] = request.form['playlist_name']

    db[collection_name].insert_one(new_document)

    return redirect(url_for('index'))


@app.route('/delete_document', methods=['POST'])
def delete_document():
    document_id = request.form['document_id']
    collection_name = request.form['collection_name']

    if collection_name not in ['artists', 'albums', 'songs', 'playlists']:
        return redirect(url_for('index'))

    db[collection_name].delete_one({'_id': ObjectId(document_id)})

    return redirect(url_for('index'))


@app.route('/update_document', methods=['POST'])
def update_document():
    document_id = ObjectId(request.form['document_id'])
    collection_name = request.form['collection_name']

    document = db[collection_name].find_one({'_id': document_id})

    if not document:
        return "Error: Document does not exist", 400

    if collection_name not in ['artists', 'albums', 'songs', 'playlists']:
        return redirect(url_for('index'))

    updates = {}
    for key, value in request.form.items():
        if key != 'document_id' and key != 'collection_name' and value:
            if key == 'artist_name' or key == 'album_name' or key == 'playlist_name':
                updates['name'] = value
            elif key == 'song_title':
                updates['title'] = value
            elif key == 'song_length':
                updates['length'] = value
            elif key == 'song_rating':
                updates['rating'] = value
            elif key == 'song_yt_link':
                updates['yt_link'] = value
            elif key == 'song_artist_id':
                updates['artist_id'] = value
            elif key == 'song_album_id':
                updates['album_id'] = value
            elif key == 'playlist_songs':
                updates['songs'] = value

    print(document_id)
    print(updates)

    result = db[collection_name].update_one({'_id': document_id}, {'$set': updates})

    if result.matched_count == 0:
        return "Error: No document found with given id", 400

    return redirect(url_for('index'))
