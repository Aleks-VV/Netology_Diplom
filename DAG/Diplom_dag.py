"""

"""
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
    """
    В бакете хранятся несколько видов файлов статистики:
    до 01.01.2017
    202206-citbike-tripdata.csv.zip
    после 01.01.2017
    202206-citbike-tripdata.csv.zip полный вариант
    JC-202206-citibike-tripdata.csv.zip урезанный вариант

    Для полноты анализа данных будем брать полные данные и отсекать урезанные данные т.е. файлы  с JC в имени.
    Данные выгружаются каждый месяц, но в произвольный день.
    Для поиска новых фалов нам нужно знать сколько всего фалов нужных нам данных находятся в хранилище.
    Для упрощения и чтоб не делать лишние проверки: число файлов не может быть больше чем число месяцев в году
    на текущую дату если смотрим текущий год и не более 12 месяцев если смотри предыдущие года.
    Например: берем 2022 год, загружаем весь список имен файлов с хранилища отфильтровываем его по году
    и полноте данных. Получаем 6 файлов за 6 месяцев. Переменная mo возвращает значение 6.
    """
    conn = boto3.client('s3', region_name=cfg_key['region_name'], aws_access_key_id=cfg_key['aws_access_key_id'],
                        aws_secret_access_key=cfg_key['aws_secret_access_key'])
    mo = 0
    for key in conn.list_objects(Bucket=SRC_BUCKET)['Contents']:
        if 'JC' not in key['Key'] and YR in key['Key']:
            mo = mo + 1
    print('returning max month {}'.format(mo))
    return mo


def copy_and_unzip_s3(**context):
    """
    Загружаем отсутствующие архивные файлы с полной выгрузкой из S3 бакета и распаковываем их локально и в свой S3 бакет
    """
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


"""
        try:
            response = s3_resource.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=context['destbucket'],
                Key=context['destkey']
            )
            print('uploaded to s3 {}'.format(filename))
        except Exception as e:
            print(e)
"""


class ClickHouseConnection:

    def get_connection(connection_name='clickhouse'):
        connection = None
        if ClickHouseConnection.connection:
            return connection
        db_props = BaseHook.get_connection('clickhouse')
        ClickHouseConnection.connection = Client(db_props.host, settings=settings)
        return ClickHouseConnection.connection


def iter_csv(filename):
    """
    Функция преобразует типы данных и формат времени соответствующий типу данных таблицы ClickHouse и формату даты
    используемому в таблицах  ClickHouse и понятному плагину clickhouse_driver. Плагин clickhouse_driver не понимает
    формат данных типа 00:07:07.5810 когда после точки более 3 чисел.
    """
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
    """
    Открываем csv файл и загружаем в таблицу ClickHouse.
    """
    ch_connection = ClickHouseConnection.get_connection()
    logging.info("CLIENT ADDED")

    # ch_connection.execute("DROP DATABASE IF EXISTS citibike")
    # logging.info("DROP DATABASE IF EXISTS citibike")

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
    """
    Делаем запросы к базе ClickHouse по перечню заданий и сохраняем в csv.
    """
    ch_connection = ClickHouseConnection.get_connection()
    logging.info("CLIENT ADDED")

    # Количество поездок в день за определенный месяц
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
    df1.to_csv(f'{context["destfilepathmetrics"]}-export-data-trip-count.csv', index=False)
    """
    df1.to_csv("s3://" + context['destbucket'] + context["destkeymetrics"] + "-export-data-trip-count.csv",
              storage_options={'key': cfg_key['aws_access_key_id'],
                               'secret': cfg_key['aws_secret_access_key']})
    """
    # Cредняя продолжительность поездок в день за месяц
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
    df2.to_csv(f'{context["destfilepathmetrics"]}-export-data-tripduration-avg.csv', index=False)
    """
    df1.to_csv("s3://" + context['destbucket'] + context["destkeymetrics"] + "-export-data-tripduration-avg.csv",
              storage_options={'key': cfg_key['aws_access_key_id'],
                               'secret': cfg_key['aws_secret_access_key']})
    """

    # Распределение поездок пользователей, разбитых по категории «gender» за месяц
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
    df3.to_csv(f'{context["destfilepathmetrics"]}-export-data-trip-by-gender.csv', index=False)
    """
    df1.to_csv("s3://" + context['destbucket'] + context["destkeymetrics"] + "-export-data-trip-by-gender.csv",
              storage_options={'key': cfg_key['aws_access_key_id'],
                               'secret': cfg_key['aws_secret_access_key']})
    """

    # Все вместе в одной таблице
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
    df4.to_csv(f'{context["destfilepathmetrics"]}-export-data-tripdata-full.csv', index=False)
    """
    df1.to_csv("s3://" + context['destbucket'] + context["destkeymetrics"] + "-export-data-tripdata-full.csv",
              storage_options={'key': cfg_key['aws_access_key_id'],
                               'secret': cfg_key['aws_secret_access_key']})
    """


def disable_task_if_success(**context):
    """
    При успешной обработке файла и выгрузки отчетов сохраняем файл с результатом выгрузки для дальнейшего 
    исключения из обработки 
    """
    with open(context['destlogfilename'], 'w') as f:
        f.write('Success')
    f.close()
    """s3_resource_log = boto3.resource('s3', region_name=cfg_key['region_name'], aws_access_key_id=cfg[
    'aws_access_key_id'], aws_secret_access_key=cfg_key['aws_secret_access_key']) text = "Success" 
    s3_resource_log.Object(context['destbucket'], context['destlogkeyname']).put(Body=text) """


def list_bucket(bucket):
    """
    Функция поиска лог файлов с успешным выполнением заданий загрузки файлов. 
    """
    # listkey = ''
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
        '''
        # Проверяем есть ли новый файл в хранилище путем изучения лог файлов успешной обработки csv файлов.
        for key in list_bucket(DEST_BUCKET):
            if NEW_DEST_LOG_KEYNAME in key['Key']:
                    print(key['Key'])
                    logging.info(f'The file {NEW_DEST_LOG_KEYNAME} exists')
            else:
                    logging.info(f'The file {NEW_DEST_LOG_KEYNAME} does not exist')
                    ................
        '''
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
