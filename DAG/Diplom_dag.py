from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
from io import BytesIO
from pathlib import Path
import boto3
import zipfile
import logging
import json
import os
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
from csv import DictReader
from datetime import datetime
import pandas as pd

cfg = json.load(open(os.path.join(os.path.dirname(__file__), 'citibike.config.json'), "r"))
cfg_key = json.load(open(os.path.join(os.path.dirname(__file__), 'citibike_key.config.json'), "r"))

settings = {'input_format_null_as_default': True}

YR = cfg['YR']
SRC_BUCKET = cfg['SRC_BUCKET']
SRC_KEY = cfg['SRC_KEY']
DEST_BUCKET = cfg['afbucket']
DEST_KEY = cfg['DEST_KEY']
now = datetime.now()


def find_max_month():
    conn = boto3.client('s3', region_name=cfg_key['region_name'], aws_access_key_id=cfg_key['aws_access_key_id'],
                        aws_secret_access_key=cfg_key['aws_secret_access_key'])
    mo = 0
    for key in conn.list_objects(Bucket=SRC_BUCKET)['Contents']:
        if 'JC' not in key['Key'] and YR in key['Key']:
            mo = mo + 1
    print('returning max month {}'.format(mo))
    return mo


def copy_and_unzip_s3(**context):
    s3_resource = boto3.resource('s3', region_name=cfg_key['region_name'],
                                 aws_access_key_id=cfg_key['aws_access_key_id'],
                                 aws_secret_access_key=cfg_key['aws_secret_access_key'])
    zip_obj = s3_resource.Object(bucket_name=context['bucket'], key=context['key'])
    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    print('downloaded zip {}, zipObj {}'.format(z, zipfile))
    logging.info('downloaded zip {}, zipObj {}'.format(z, zipfile))
    logging.info(f'The file {context["destfilename"]} does not exist')
    for filename in z.namelist():
        if filename.startswith("__"):
            continue
        file_info = z.getinfo(filename)
        print('interating over zip {}, zipObj {}'.format(filename, file_info))
        try:
            z.extractall(context['destfilepath'])
        except Exception as e:
            print(e)


class ClickHouseConnection:
    connection = None

    def get_connection(connection_name='clickhouse'):
        if ClickHouseConnection.connection:
            return connection
        db_props = BaseHook.get_connection(connection_name)
        ClickHouseConnection.connection = Client(db_props.host, settings=settings)
        return ClickHouseConnection.connection


def iter_csv(filename):
    converters = {
        'tripduration': int,
        'start_station_id': int,
        'end_station_id': int,
        'bike_id': int,
        'gender': int,
        'start_station_lat': float,
        'start_station_lon': float,
        'end_station_lat': float,
        'end_station_lon': float,
        'starttime': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'),
        'stoptime': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
    }

    with open(filename, 'r') as f:
        reader = DictReader(f, fieldnames=['tripduration', 'starttime', 'stoptime', 'start_station_id',
                                           'start_station_name', 'start_station_lat', 'start_station_lon',
                                           'end_station_id', 'end_station_name', 'end_station_lat', 'end_station_lon',
                                           'bike_id', 'usertype', 'birthyear', 'gender'])
        next(reader, None)
        for line in reader:
            try:
                yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}
            except Exception as e:
                print(e)
                print(line)
    f.close()


def copy_to_clickhouse(**context):
    ch_connection = ClickHouseConnection.get_connection()
    logging.info("CLIENT ADDED")

    ch_connection.execute("CREATE DATABASE IF NOT EXISTS citibike")
    logging.info("CREATE DATABASE citibike")

    ch_connection.execute(
        '''CREATE TABLE IF NOT EXISTS citibike.data_csv
            (
                `tripduration` UInt32,
                `starttime` DateTime,
                `stoptime` DateTime,
                `start_station_id` UInt32,
                `start_station_name` String,
                `start_station_lat` Float64,
                `start_station_lon` Float64,
                `end_station_id` UInt32,
                `end_station_name` String,
                `end_station_lat` Float64,
                `end_station_lon` Float64,
                `bike_id` UInt32,
                `usertype` String,
                `birthyear` String,
                `gender` UInt32
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(starttime)
            ORDER BY (gender, starttime)
        ''')
    logging.info("CREATE TABLE citibike.data_csv")

    ch_connection.execute('INSERT INTO citibike.data_csv VALUES', iter_csv(context['destfilename']))
    logging.info(f"INSERT {context['destfilename']} INTO citibike.data_csv")


def make_metrics_from_clickhouse(**context):
    ch_connection = ClickHouseConnection.get_connection()
    logging.info("CLIENT ADDED")

    result1 = ch_connection.execute(
        f"select toDate(starttime) AS date_local  , count(*) AS trip_on_days from citibike.data_csv dc WHERE MONTH("
        f"`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local  order by date_local")
    logging.info(
        f"select toDate(starttime) AS date_local  , count(*) AS trip_on_days from citibike.data_csv dc WHERE MONTH("
        f"`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local  order by date_local")
    print(result1)
    df1 = pd.DataFrame(result1)
    df1.to_csv(f'{context["destfilepathmetrics"]}-export-data-trip-count.csv',
               header=['date', 'trip_on_days'], index=False)

    result2 = ch_connection.execute(
        f"select toDate(starttime) AS date_local  , round(avg (tripduration)) AS avg_tripduration_on_days_sec, "
        f"formatDateTime(CAST(toUInt32(avg_tripduration_on_days_sec) AS DATETIME), '%H:%M:%S') AS "
        f"avg_tripduration_on_days_hms from citibike.data_csv dc WHERE MONTH(`starttime`) = MONTH(toDate('"
        f"{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('{context['startdate']}')) group by "
        f"date_local  order by date_local")
    print(result2)
    logging.info(
        f"select toDate(starttime) AS date_local  , round(avg (tripduration)) AS avg_tripduration_on_days_sec, "
        f"formatDateTime(CAST(toUInt32(avg_tripduration_on_days_sec) AS DATETIME), '%H:%M:%S') AS "
        f"avg_tripduration_on_days_hms from citibike.data_csv dc WHERE MONTH(`starttime`) = MONTH(toDate('"
        f"{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('{context['startdate']}')) group by "
        f"date_local  order by date_local")
    df2 = pd.DataFrame(result2)
    df2.to_csv(f'{context["destfilepathmetrics"]}-export-data-tripduration-avg.csv',
               header=['date', ' avg_tripduration_on_days_sec', 'avg_tripduration_on_days_hms'], index=False)

    result3 = ch_connection.execute(
        f"select toDate(starttime) AS date_local, gender, count(starttime) AS trip_on_days from citibike.data_csv dc "
        f"WHERE MONTH(`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local, gender  order by date_local")
    logging.info(
        f"select toDate(starttime) AS date_local, gender, count(starttime) AS trip_on_days from citibike.data_csv dc "
        f"WHERE MONTH(`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local, gender  order by date_local")
    print(result3)
    df3 = pd.DataFrame(result3)
    df3.to_csv(f'{context["destfilepathmetrics"]}-export-data-trip-by-gender.csv',
               header=['date', ' gender', 'trip_on_days'], index=False)

    result4 = ch_connection.execute(
        f"select toDate(starttime) AS date_local, gender, round(avg (tripduration)) AS avg_tripduration_on_days_sec, "
        f"formatDateTime(CAST(toUInt32(avg_tripduration_on_days_sec) AS DATETIME), '%H:%M:%S') AS "
        f"avg_tripduration_on_days_hms, count(starttime) AS trip_on_days from citibike.data_csv dc WHERE MONTH("
        f"`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local, gender  order by date_local")
    print(result4)
    logging.info(
        f"select toDate(starttime) AS date_local, gender, round(avg (tripduration)) AS avg_tripduration_on_days_sec, "
        f"formatDateTime(CAST(toUInt32(avg_tripduration_on_days_sec) AS DATETIME), '%H:%M:%S') AS "
        f"avg_tripduration_on_days_hms, count(starttime) AS trip_on_days from citibike.data_csv dc WHERE MONTH("
        f"`starttime`) = MONTH(toDate('{context['startdate']}')) and YEAR(`starttime`) = YEAR(toDate('"
        f"{context['startdate']}')) group by date_local, gender  order by date_local")
    df4 = pd.DataFrame(result4)
    df4.to_csv(f'{context["destfilepathmetrics"]}-export-data-tripdata-full.csv',
               header=['date', ' gender', 'avg_tripduration_on_days_sec', 'avg_tripduration_on_days_hms',
                       'trip_on_days'], index=False)
    
def disable_task_if_success(**context):
    with open(context['destlogfilename'], 'w') as f:
        f.write('Success')
    f.close()


def list_bucket(bucket):
    conn = boto3.client('s3', region_name=cfg_key['region_name'], aws_access_key_id=cfg_key['aws_access_key_id'],
                        aws_secret_access_key=cfg[
                            'aws_secret_access_key'])  
    listkey = conn.list_objects(Bucket=bucket)['Contents']
    for key in listkey:
        if 'tripdata.log' in key['Key']:
            print(key['Key'])
    return listkey


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'schedule_interval': '@daily',
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Citibike_Ridership_Analytics_Diplom',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=datetime(now.year, now.month, now.day, now.hour),
    tags=['s3', 'Citibike', 'ClickHouse'],
) as dag:
    start = EmptyOperator(
        task_id='start',
        dag=dag)

    for i in range(1, find_max_month() + 1):
        NEW_SRC_KEY = YR + str(i).zfill(2) + SRC_KEY
        NEW_DEST_KEY = 'citibike/csv/' + YR + str(i).zfill(2) + DEST_KEY
        NEW_DEST_KEY_METRICS = '/citibike/results/' + YR + str(i).zfill(2) + DEST_KEY
        NEW_DEST_FILE_NAME = cfg['LOCAL_PATH'] + '/' + YR + str(i).zfill(2) + DEST_KEY
        NEW_DEST_FILE_PATH = cfg['LOCAL_PATH']
        NEW_DEST_LOG_FILENAME = cfg['LOCAL_PATH'] + '/logs/' + YR + str(i).zfill(2) + '-citibike-tripdata.log'
        NEW_DEST_LOG_KEYNAME = '/citibike/csv/logs/' + YR + str(i).zfill(2) + '-citibike-tripdata.log'
        NEW_DEST_FILE_PATH_METRICS = cfg['LOCAL_PATH'] + '/metrics/' + YR + str(i).zfill(2)
        PATH = Path(NEW_DEST_LOG_FILENAME)
        if PATH.is_file():
            logging.info(f'The file {NEW_DEST_LOG_FILENAME} exists')
        else:
            logging.info(f'The file {NEW_DEST_LOG_FILENAME} does not exist')
            copyAndTransformS3File = PythonOperator(
                task_id='copy_and_unzip_s3_' + str(i).zfill(2),
                python_callable=copy_and_unzip_s3,
                op_kwargs={'bucket': SRC_BUCKET, 'key': NEW_SRC_KEY, 'destbucket': DEST_BUCKET, 'destkey': NEW_DEST_KEY,
                           'destfilename': NEW_DEST_FILE_NAME, 'destfilepath': NEW_DEST_FILE_PATH},
                dag=dag)
            CopyToClickHouse = PythonOperator(
                task_id='copy_to_ClickHouse' + str(i).zfill(2),
                python_callable=copy_to_clickhouse,
                op_kwargs={'destkey': NEW_DEST_KEY, 'destfilename': NEW_DEST_FILE_NAME,
                           'destfilepath': NEW_DEST_FILE_PATH},
                dag=dag)
            MakeMetricsFromClickHouse = PythonOperator(
                task_id='make_metrics_from_ClickHouse' + str(i).zfill(2),
                python_callable=make_metrics_from_clickhouse,
                op_kwargs={'destbucket': DEST_BUCKET, 'startdate': YR + '-' + str(i).zfill(2) + '-01',
                           'enddate': YR + '-' + str(i + 1).zfill(2) + '-01',
                           'destfilepathmetrics': NEW_DEST_FILE_PATH_METRICS, 'destkeymetrics': NEW_DEST_KEY_METRICS},
                dag=dag)
            DisableTaskIfSuccess = PythonOperator(
                task_id='disable_task_if_success' + str(i).zfill(2),
                python_callable=disable_task_if_success,
                op_kwargs={'destbucket': DEST_BUCKET, 'destlogfilename': NEW_DEST_LOG_FILENAME,
                           'destlogkeyname': NEW_DEST_LOG_KEYNAME},
                dag=dag)
            start >> copyAndTransformS3File >> CopyToClickHouse >> MakeMetricsFromClickHouse >> DisableTaskIfSuccess
