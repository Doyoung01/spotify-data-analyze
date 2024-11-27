from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import redshift_connector
from io import StringIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['example'])
def global_charts_elt():

    @task
    def create_table_from_csv(bucket_name, file_key, table_name):
        
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

        # S3에서 CSV 파일 읽기
        s3_hook = S3Hook(aws_conn_id='de4_s3_conn')
        file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)

        # CSV 파일을 DataFrame으로 변환
        df = pd.read_csv(StringIO(file_content))

        # DataFrame의 컬럼과 데이터 타입 추출
        columns = df.columns
        dtypes = df.dtypes

        # 데이터 타입 매핑
        dtype_mapping = {
            'int64': 'BIGINT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)'
        }

        # Redshift 테이블 생성 쿼리 작성
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for col, dtype in zip(columns, dtypes):
            redshift_type = dtype_mapping.get(str(dtype), 'VARCHAR(255)')
            create_table_query += f"{col} {redshift_type}, "
        create_table_query = create_table_query.rstrip(', ') + ");"

        # Redshift 연결 및 테이블 생성
        conn = get_redshift_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(create_table_query)
            conn.commit()

            # S3에서 Redshift로 데이터 복사
            copy_query = f"""
            COPY {table_name}
            FROM 's3://{bucket_name}/{file_key}'
            IAM_ROLE 'arn:aws:iam::442426885723:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T142119'
            CSV
            IGNOREHEADER 1
            DELIMITER ',';
            """
            cursor.execute(copy_query)
            conn.commit()
            print(f"데이터가 {table_name} 테이블로 성공적으로 복사되었습니다.")
        except Exception as e:
            print(f"오류 발생: {e}")
        finally:
            cursor.close()
            conn.close()

    create_table_task = create_table_from_csv(bucket_name='de4project', file_key='global-charts/top100global.csv', table_name='raw_data.top100global')

    # 다음 DAG을 트리거하는 태스크 추가
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_make_dw_dag',
        trigger_dag_id='make_dw',  # 다음 DAG의 ID
        wait_for_completion=True,
        reset_dag_run=True,
    )

    create_table_task >> trigger_next_dag

global_charts_elt_dag = global_charts_elt()