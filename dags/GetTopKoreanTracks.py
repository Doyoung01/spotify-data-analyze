from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import utils.spotifyUtils as su
import pandas as pd
import boto3

@task
def top_fifty_korean_tracks(output_path):
    # Daily Chart TOP 50 in Korea
    playlist_id = "37i9dQZEVXbNxXF4SkHj9F"
    params = {
        "country": "KR",
        "fields": "tracks(total, items(track(artists(name, id), external_urls.spotify, name, popularity)))" 
    }

    json = get_playlist(playlist_id, params)

    if json['tracks']['total'] != 50:
        print("Incorrect number of tracks")
        raise
    else:
        tracks = []
        artists_id = []

        for item in json['tracks']['items']:
            track = item['track']
            artists = [artists['name'] for artists in track['artists']]
            artists_id.extend([artists['id'] for artists in track['artists']])
            tracks.append({
                "title": track['name'],
                "artist": ', '.join(artists),
                "popularity": track['popularity'],
                "url": track['external_urls']['spotify'],
                "date": datetime.now().date()
            })

        df = pd.DataFrame(tracks)
        df.to_csv(output_path, index=False)
        
        return set(artists_id)


def get_playlist(playlist_id, params):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    
    json = su.send_request(params, url)

    if json:
        return json
    else:
        print("Failed to get playlist")
        raise


@task
def average_time_of_artists(artists_id, output_path):
    data = []

    df = pd.DataFrame(columns=["artist", "duration_ms"])
    for id in artists_id:
        json = get_artist_top_tracks(id)
        name = get_artist(id)['name']
        duration = 0
        for track in json['tracks']:
            duration += track['duration_ms']
        if len(json['tracks']) != 0:
            duration = duration / len(json['tracks'])

        data.append({"artist": name, "duration_ms": duration, "date": datetime.now().date()})

    df = pd.DataFrame(data)
    df.sort_values(by="duration_ms", ascending=False, inplace=True)
    df.to_csv(output_path, index=False)


def get_artist_top_tracks(id):
    url = f"https://api.spotify.com/v1/artists/{id}/top-tracks"

    json = su.send_request({}, url)

    if json:
        return json
    else:
        print("Failed to get artist's top tracks")
        raise


def get_artist(id):
    url = f"https://api.spotify.com/v1/artists/{id}"

    json = su.send_request({}, url)

    if json:
        return json
    else:
        print("Failed to get artist")
        raise


@task
def get_kpop_artists():
    params = {
        "q": "genre:\"k-pop\"",
        "type": "artist",
        "market": "KR",
        "offset": 0,
        "limit": 50
    }

    json = search_in_spotify(params)
    
    artists_id = set()
    offset = 0
    while len(artists_id) < 50:
        params['offset'] = offset
        json = search_in_spotify(params)
        items = json.get('artists', {}).get('items', [])
        for item in items:
            artists_id.add(item['id'])
        if not items or len(artists_id) >= 50:
            break
        offset += 50
    
    return list(artists_id)


def search_in_spotify(params):
    url = f'https://api.spotify.com/v1/search'

    json = su.send_request(params, url)

    if json:
        return json
    else:
        print("Failed to search in Spotify")
        raise


@task
def get_artist_follwers(artists_id, output_path):
    data = []
    for id in artists_id:
        json = get_artist(id)
        data.append({
            "artist": json['name'],
            "followers": json['followers']['total'],
            "date": datetime.now().date()
        })
    
    df = pd.DataFrame(data)
    df.sort_values(by="followers", ascending=False, inplace=True)
    df.to_csv(output_path, index=False)


@task
def upload_file_to_s3(bucket_name, local_file_path, s3_folder, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    current_date = datetime.now().strftime('%Y-%m-%d')
    file_name = local_file_path.split('/')[-1]

    s3_key = f"{s3_folder}/{current_date}/{file_name}"

    s3_client.upload_file(local_file_path, bucket_name, s3_key)


with DAG(
    dag_id='spotify_kpop_data_analyze',
    start_date=datetime(2024, 11, 23),
    schedule='0 0 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    path = [
        '/tmp/average_time_of_top_fifty_korean_tracks.csv',
        '/tmp/average_time_of_top_fifty_artists.csv',
        '/tmp/followers_of_top_fifty_artists.csv'
    ]

    average_korean_tracks = average_time_of_artists(
        top_fifty_korean_tracks(output_path='/tmp/top_fifty_korean_tracks.csv'),
        output_path=path[0]
    )
    average_kpop_artists = average_time_of_artists(
        get_kpop_artists(),
        output_path=path[1]
    )
    followers_kpop_artists = get_artist_follwers(
        get_kpop_artists(),
        output_path=path[2]
    )

    upload_avg_korean_tracks = upload_file_to_s3(
        bucket_name='de4project',
        local_file_path=path[0],
        s3_folder='kpop-idol-data',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )

    upload_avg_kpop_artists = upload_file_to_s3(
        bucket_name='de4project',
        local_file_path=path[1],
        s3_folder='kpop-idol-data',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )

    upload_followers = upload_file_to_s3(
        bucket_name='de4project',
        local_file_path=path[2],
        s3_folder='kpop-idol-data',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )

    upload_korean_tracks = upload_file_to_s3(
        bucket_name='de4project',
        local_file_path='/tmp/top_fifty_korean_tracks.csv',
        s3_folder='kpop-idol-data',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )

    average_korean_tracks >> [upload_korean_tracks, upload_avg_korean_tracks]
    average_kpop_artists >> upload_avg_kpop_artists
    followers_kpop_artists >> upload_followers
