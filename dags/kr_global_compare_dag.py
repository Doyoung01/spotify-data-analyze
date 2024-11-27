from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import os
import time
import boto3
from psycopg2 import sql
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# AWS S3 설정
S3_BUCKET_NAME = "de4project"
S3_UPLOAD_PATH = "global-kr-charts/"

# Redshift 설정
conn = BaseHook.get_connection('de4_redshift_conn')
host = conn.host
port = conn.port
database = conn.schema
user = conn.login
password = conn.password

# 파일 경로
DOWNLOAD_DIR = "/opt/airflow/dags/data"

def get_latest_thursday_date():
    """Airflow 실행 날짜 기준으로 가장 가까운 목요일 날짜 반환"""
    today = datetime.now()
    # 목요일은 3번째 요일 (월=0, 화=1, ..., 목=3)
    offset = (today.weekday() - 3) % 7  # 가장 가까운 목요일 계산
    latest_thursday = today - timedelta(days=offset)
    return latest_thursday.strftime("%Y-%m-%d")

# 최신 목요일 날짜를 기반으로 파일 이름 생성
LATEST_THURSDAY_DATE = get_latest_thursday_date()
KR_CHART_CSV = os.path.join(DOWNLOAD_DIR, f"regional-kr-weekly-{LATEST_THURSDAY_DATE}.csv")
GLOBAL_CHART_CSV = os.path.join(DOWNLOAD_DIR, f"regional-global-weekly-{LATEST_THURSDAY_DATE}.csv")

# 다운로드 경로 설정
prefs = {"download.default_directory": DOWNLOAD_DIR}
options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", prefs)

def rename_downloaded_file(original_pattern, renamed_path):
    """Selenium으로 다운로드된 파일을 지정된 이름으로 변경"""
    time.sleep(5)  # 다운로드 완료 대기
    for file in os.listdir(DOWNLOAD_DIR):
        if original_pattern in file:
            original_file_path = os.path.join(DOWNLOAD_DIR, file)
            os.rename(original_file_path, renamed_path)
            print(f"{original_file_path} -> {renamed_path}")
            return
    raise FileNotFoundError(f"'{original_pattern}' 패턴의 파일을 찾을 수 없습니다.")

def download_spotify_charts():
    """Spotify Charts 페이지에서 파일 다운로드"""
    # Selenium WebDriver 설정
    remote_webdriver = 'remote_chromedriver'
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
        # Spotify Charts 페이지 접속
        driver.get("https://accounts.spotify.com/ko/login?continue=https%3A%2F%2Fcharts.spotify.com/login")
        time.sleep(2)

        try:
            # 아이디 입력
            id_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="login-username"]'))
            )
            id_input.send_keys('your_spotify_ID')
            print("아이디 입력 완료")

            # 비밀번호 입력
            password_input = driver.find_element(By.XPATH, '//*[@id="login-password"]')
            password_input.send_keys('your_spotify_PW')
            print("비밀번호 입력 완료")

            # 로그인 버튼 클릭
            login_submit = driver.find_element(By.XPATH, '//*[@id="login-button"]')
            login_submit.click()
            print("로그인 완료")
        except Exception as e:
            print(f"로그인 중 오류 발생: {e}")

        time.sleep(5)

        # CSV 파일 다운로드
        try:
            # 최신 주간 차트 페이지 접속
            driver.get(f"https://charts.spotify.com/charts/view/regional-kr-weekly/latest")
            time.sleep(2)

            # CSV 다운로드 버튼 클릭
            csv_download_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="__next"]/div/div[3]/div/div/div[2]/span/span/button'))
            )
            csv_download_button.click()
            rename_downloaded_file("regional-kr-weekly", KR_CHART_CSV)
            time.sleep(2)

            driver.get(f"https://charts.spotify.com/charts/view/regional-global-weekly/latest")
            time.sleep(2)

            csv_download_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="__next"]/div/div[3]/div/div/div[2]/span/span/button'))
            )
            csv_download_button.click()
            rename_downloaded_file("regional-global-weekly", GLOBAL_CHART_CSV)
            time.sleep(2)

            print("CSV 파일 다운로드 완료")
        except Exception as e:
            print(f"CSV 다운로드 중 오류 발생: {e}")
        print("Spotify 차트 다운로드 완료")


def upload_to_s3():
    """S3에 파일 업로드"""
    # S3Hook 생성 (Connection ID 사용)
    s3_hook = S3Hook(aws_conn_id="aws_s3_default")

    # 업로드할 파일 리스트
    files = [KR_CHART_CSV, GLOBAL_CHART_CSV]
    
    for file_path in files:
        # 파일명 추출
        file_name = os.path.basename(file_path)
        s3_key = os.path.join(S3_UPLOAD_PATH, file_name)

        # S3에 파일 업로드
        s3_hook.load_file(
            filename=file_path,
            bucket_name=S3_BUCKET_NAME,
            key=s3_key,
            replace=True  # 기존 파일 덮어쓰기 허용
        )
        print(f"{file_name} 파일이 S3에 업로드되었습니다.")


def process_charts():
    """KR 차트와 글로벌 차트를 로드 및 매칭"""
    kr_chart = pd.read_csv(KR_CHART_CSV)
    global_chart = pd.read_csv(GLOBAL_CHART_CSV)

    kr_top_200 = kr_chart[['rank', 'track_name', 'artist_names']].head(200)
    kr_top_200['global_rank'] = None

    for idx, row in kr_top_200.iterrows():
        track_name = row['track_name']
        artist_name = row['artist_names']
        match = global_chart[
            (global_chart['track_name'] == track_name) &
            (global_chart['artist_names'] == artist_name)
        ]
        if not match.empty:
            kr_top_200.at[idx, 'global_rank'] = match.iloc[0]['rank']

    kr_top_200.to_csv(os.path.join(DOWNLOAD_DIR, "kr_global_matched.csv"), index=False)
    print("매칭 완료")


def save_to_redshift():
    """Redshift에 데이터 저장"""
    cursor = conn.cursor()

    table_name = "kr_top200_global_rankings"
    cursor.execute(sql.SQL(f"CREATE SCHEMA IF NOT EXISTS {database}"))

    # 기존 테이블 삭제 후 다시 생성
    cursor.execute(f"""
    DROP TABLE IF EXISTS {database}.{table_name};
    """)
    print(f"{database}.{table_name} 테이블 삭제 완료")

    cursor.execute(f"""
    CREATE TABLE {database}.{table_name} (
        kr_rank INT,
        track_name VARCHAR(500),
        artist_name VARCHAR(500),
        global_rank INT
    );
    """)
    print(f"{database}.{table_name} 테이블 생성 완료")

    matched_data = pd.read_csv(os.path.join(DOWNLOAD_DIR, "kr_global_matched.csv"))
    for _, row in matched_data.iterrows():
        cursor.execute(f"""
        INSERT INTO {database}.{table_name} (kr_rank, track_name, artist_name, global_rank)
        VALUES (%s, %s, %s, %s);
        """, (row['rank'], row['track_name'], row['artist_names'], row['global_rank']))

    conn.commit()
    cursor.close()
    conn.close()
    print("Redshift 저장 완료")


# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_redshift_pipeline",
    default_args=default_args,
    description="Spotify 데이터를 Redshift로 적재하는 파이프라인",
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_spotify_charts",
        python_callable=download_spotify_charts,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    process_task = PythonOperator(
        task_id="process_charts",
        python_callable=process_charts,
    )

    save_task = PythonOperator(
        task_id="save_to_redshift",
        python_callable=save_to_redshift,
    )

    download_task >> upload_task >> process_task >> save_task
