from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import boto3
import re

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_execution_dates_from_s3(bucket_name, prefix):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY')
    )
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    dates = set()
    for obj in response.get('Contents', []):
        match = re.match(rf"{prefix}(\d{{4}}-\d{{2}}-\d{{2}})/", obj['Key'])
        if match:
            dates.add(match.group(1))
    return sorted(list(dates))

@task
def load_chart(s3_key, execution_dates, schema, table='korean_chart'):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                title VARCHAR(255),
                artist VARCHAR(255),
                popularity INT,
                url VARCHAR(255),
                date DATE DEFAULT GETDATE()
            );''')
        cur.execute(f"DELETE FROM {schema}.{table}")

        for date in execution_dates:
            s3_path = f"s3://{s3_key}/{date}/top_fifty_korean_tracks.parquet"
            cur.execute(f'''
                COPY {schema}.{table}
                FROM '{s3_path}'
                IAM_ROLE '{Variable.get('REDSHIFT_IAM_ROLE')}'
                FORMAT AS PARQUET;
            ''')
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise

@task
def load_avg_time_chart(s3_key, execution_dates, schema, table='avg_time_chart'):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                artist VARCHAR(255),
                duration_ms INT,
                date DATE DEFAULT GETDATE()
            );''')
        cur.execute(f"DELETE FROM {schema}.{table}")

        for date in execution_dates:
            s3_path = f"s3://{s3_key}/{date}/average_time_of_top_fifty_korean_tracks.parquet"
            cur.execute(f'''
                COPY {schema}.{table}
                FROM '{s3_path}'
                IAM_ROLE '{Variable.get('REDSHIFT_IAM_ROLE')}'
                FORMAT AS PARQUET;
            ''')
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise

@task
def load_avg_time_kpop(s3_key, execution_dates, schema, table='avg_time_kpop'):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                artist VARCHAR(255),
                duration_ms INT,
                date DATE DEFAULT GETDATE()
            );''')
        cur.execute(f"DELETE FROM {schema}.{table}")

        for date in execution_dates:
            s3_path = f"s3://{s3_key}/{date}/average_time_of_top_fifty_artists.parquet"
            cur.execute(f'''
                COPY {schema}.{table}
                FROM '{s3_path}'
                IAM_ROLE '{Variable.get('REDSHIFT_IAM_ROLE')}'
                FORMAT AS PARQUET;
            ''')
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise

@task
def load_followers(s3_key, execution_dates, schema, table='followers_kpop'):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                artist VARCHAR(255),
                followers INT,
                date DATE DEFAULT GETDATE()
            );''')
        cur.execute(f"DELETE FROM {schema}.{table}")

        for date in execution_dates:
            s3_path = f"s3://{s3_key}/{date}/followers_of_top_fifty_artists.parquet"
            cur.execute(f'''
                COPY {schema}.{table}
                FROM '{s3_path}'
                IAM_ROLE '{Variable.get('REDSHIFT_IAM_ROLE')}'
                FORMAT AS PARQUET;
            ''')
        cur.execute("COMMIT;")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id='redshift_data_load',
    start_date=datetime(2024, 1, 1),
    schedule='0 1 * * *',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    max_active_runs = 1,
    catchup=False,
):
    bucket_name = 'de4project'
    prefix = 'kpop-idol-data'
    schema = 'raw_data'
    s3_key = bucket_name + '/' + prefix

    execution_dates = get_execution_dates_from_s3(bucket_name, prefix)
    load_chart(s3_key, execution_dates, schema)
    load_avg_time_chart(s3_key, execution_dates, schema)
    load_avg_time_kpop(s3_key, execution_dates, schema)
    load_followers(s3_key, execution_dates, schema)