from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import base64
import time
import random
from io import StringIO
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval=timedelta(days=7), catchup=False, tags=['example'])
def global_charts_etl():
    
    @task
    def get_access_token():
        client_id = Variable.get("spotify_client_id")
        client_secret = Variable.get("spotify_client_secret")
        endpoint = "https://accounts.spotify.com/api/token"
        encoded = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('ascii')
        headers = {"Authorization": f"Basic {encoded}"}
        payload = {"grant_type": "client_credentials"}

        response = requests.post(endpoint, data=payload, headers=headers)
        if response.status_code == 200:
            access_token = json.loads(response.text)['access_token']
            return access_token
        else:
            raise Exception("Failed to get access token")

    @task
    def etl_process(access_token, ti):
        bucket_name = 'de4project'
        headers = {"Authorization": f"Bearer {access_token}"}

        # download_from_s3 기능을 etl_process 내부로 이동
        def download_from_s3(bucket_name, file_key):
            s3_hook = S3Hook(aws_conn_id='de4_s3_conn')
            file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
            df = pd.read_csv(StringIO(file_content))
            return df
        
        def upload_to_s3(bucket_name, file_key, file_content):
            # logging.info("Top 100 Global DataFrame:\n%s", file_content.head().to_string())
            s3_hook = S3Hook(aws_conn_id='de4_s3_conn')
            s3_hook.load_string(string_data=file_content, key=file_key, bucket_name=bucket_name, replace=True)
        
        def fetch_track_info(track_id, headers):
            track_data = []
            try:
                audio_features_response = requests.get(
                f"https://api.spotify.com/v1/audio-features/{track_id}", headers=headers)

                if audio_features_response.status_code == 200:
                    track_features = audio_features_response.json()
                    track_data.append({
                        'track_id': track_id,
                        'duration_ms': track_features.get('duration_ms'),
                        'tempo': track_features.get('tempo'),
                        'danceability': track_features.get('danceability'),
                        'energy': track_features.get('energy'),
                        'valence': track_features.get('valence')
                    })
                    time.sleep(random.uniform(1, 2))
                elif audio_features_response.status_code == 429:
                    retry_after = int(audio_features_response.headers.get("Retry-After", 5))
                    time.sleep(retry_after)
                else:
                        print(f"오류 발생: 오디오 특징 {audio_features_response.status_code}")
            except Exception as e:
                print(f"예외 발생: {e}")
                
            return track_data

        # S3에서 데이터 다운로드
        weekly_chart_df = download_from_s3(bucket_name=bucket_name, file_key='global-charts/weekly_charts.csv')
        
        # 슬라이싱
        weekly_chart_df = weekly_chart_df[:100]
        
        weekly_chart_df['track_id'] = weekly_chart_df['uri'].str.split(':').str[-1]

        # track_info_df도 동일한 방식으로 가져옴
        track_info_df = download_from_s3(bucket_name=bucket_name, file_key='global-charts/track_info.csv')

        if track_info_df.empty:
            track_data = []
            for track_id in weekly_chart_df['track_id']:
                track_data.extend(fetch_track_info(track_id, headers))
            track_info_df = pd.DataFrame(track_data)
            upload_to_s3(bucket_name, 'global-charts/track_info.csv', track_info_df.to_csv(index=False))

        else:
            track_data = []
            for track_id in weekly_chart_df['track_id']:
                if track_id not in track_info_df['track_id']:
                    track_data.extend(fetch_track_info(track_id, headers))

            if track_data:
                track_df = pd.DataFrame(track_data)
                track_info_df = pd.concat([track_info_df, track_df], ignore_index=True)
                upload_to_s3(bucket_name, 'global-charts/track_info.csv', track_info_df.to_csv(index=False))

        top100global_df = pd.merge(weekly_chart_df, track_info_df, on='track_id')
        upload_to_s3(bucket_name, 'global-charts/top100global.csv', top100global_df.to_csv(index=False))

    access_token = get_access_token()
    etl_process_task = etl_process(access_token)

    # 다음 DAG을 트리거하는 태스크 추가
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_global_charts_elt_dag',
        trigger_dag_id='global_charts_elt',  # 다음 DAG의 ID
        wait_for_completion=True,
        reset_dag_run=True,
    )

    etl_process_task >> trigger_next_dag

global_charts_etl_dag = global_charts_etl()