from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import redshift_connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['example'])
def make_dw():
    
    def get_redshift_connection():
        conn = BaseHook.get_connection('de4_redshift_conn')
        host = conn.host
        port = conn.port
        database = conn.schema
        user = conn.login
        password = conn.password

        return redshift_connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    @task
    def create_trend_table():
        conn = get_redshift_connection()
        cursor = conn.cursor()
        try:
            # global_charts 스키마가 존재하지 않으면 생성
            cursor.execute("CREATE SCHEMA IF NOT EXISTS global_charts;")
            
            # 테이블이 존재하면 삭제
            cursor.execute("DROP TABLE IF EXISTS global_charts.trend;")
            cursor.execute("DROP TABLE IF EXISTS global_charts.streaming_performance;")
            cursor.execute("DROP TABLE IF EXISTS global_charts.track_features;")
            cursor.execute("DROP TABLE IF EXISTS global_charts.artist_performance;")
            cursor.execute("DROP TABLE IF EXISTS global_charts.mood_analysis;")
            
            # trend 테이블 생성
            cursor.execute("""
                CREATE TABLE global_charts.streaming_performance AS
                SELECT 
                    track_name,
                    artist_names,
                    SUM(streams) AS total_streams,
                    MAX(peak_rank) AS best_rank
                FROM raw_data.top100global
                GROUP BY track_name, artist_names;
            """)
            
            cursor.execute("""
                CREATE TABLE global_charts.track_features AS
                SELECT 
                    track_name,
                    artist_names,
                    tempo,
                    danceability,
                    energy
                FROM raw_data.top100global;
            """)
            
            cursor.execute("""
                CREATE TABLE global_charts.artist_performance AS
                SELECT 
                    artist_names,
                    SUM(streams) AS total_streams,
                    COUNT(DISTINCT track_name) AS total_tracks
                FROM raw_data.top100global
                GROUP BY artist_names;
            """)
            
            cursor.execute("""
                CREATE TABLE global_charts.mood_analysis AS
                SELECT 
                    track_name,
                    valence,
                    energy,
                    danceability
                FROM raw_data.top100global;
            """)
            conn.commit()
            print("global_charts 스키마의 테이블이 성공적으로 생성되었습니다.")
        except Exception as e:
            print(f"오류 발생: {e}")
        finally:
            cursor.close()
            conn.close()

    create_trend_table()

make_dw_dag = make_dw()