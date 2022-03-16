from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
import shutil, os
from google.cloud import storage
from airflow.operators.email_operator import EmailOperator
import sys
from airflow.utils.email import send_email
import logging


input_file_path = './dags/file_Download/'
input_csvs = './dags/input_csvs/'
processed_csvs = './dags/processed_csvs/'
client = storage.Client.from_service_account_json('**.json')

def download_file():
    logging.info('Download File Task Started')
    bucket_name = 'your_bucket_name'
    source_blob_name = 'Event_File_03_16_2022.xlsx'

    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(source_blob_name)
    blob.download_to_filename(input_file_path+'/'+source_blob_name)

    files = os.listdir(input_file_path)
    for fname in files:
        shutil.copy2(os.path.join(input_file_path, fname), input_csvs)

    logging.info('Download File Task Completed')


def excelToCSV():
    logging.info('ExcelImport Task Started')
    file_name = 'Event_File_03_16_2022.xlsx'
    file_path = input_file_path+'/'+source_blob_name
    df = pd.read_excel(file_path, sheet_name=None)
    
    for key in df.keys(): 
        print(format(key))
        df[key].to_csv(input_csvs+'\{}.csv'.format(key))

    files = os.listdir(input_file_path)

    for fname in files:
        shutil.copy2(os.path.join(input_file_path, fname), input_csvs)

    for fname in files:
        os.remove(input_file_path + fname)
    logging.info('ExcelImport Task Completed')


def transformation():
    logging.info('ApplyTransformation Task Started')
    trainDetailsDF = pd.read_csv(input_csvs+'/Train-Details.csv')
    regionDf = pd.read_csv(input_csvs+'/Region.csv')
    divisionDf = pd.read_csv(input_csvs + '/Division.csv')

    df = trainDetailsDF.merge(divisionDf, left_on='TrainId', right_on='TrainId')
    df = df.merge(regionDf, left_on='TrainId', right_on='TrainId')
    df = df[df['Event-Type'] == 'GPS']

    header = ['TrainId', 'Train-Type_x', 'Departing-Station', 'Departing-State', 'Division', 'Region',
              'Created_Date_Time_x']

    df.to_csv('./dags/processed_csvs/train-filtered-data.csv')
    files = os.listdir(input_csvs)
    for fname in files:
        shutil.copy2(os.path.join(input_csvs, fname), processed_csvs)

    for fname in files:
        os.remove(input_csvs  + fname)
    logging.info('ApplyTransformation Task Completed')

def processed():
    logging.info('Upload File Task Started')
    bucket_name = 'your_bucket_name'
    file_name = 'train-filtered-data.csv'

    source_blob_name = processed_csvs+file_name
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.upload_from_filename(source_blob_name)

    files = os.listdir(processed_csvs)
    for fname in files:
        os.remove(processed_csvs + fname)

    logging.info('Upload File Task Completed')

def notify_email(contextDict, **kwargs):
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    body = """
    Hi All, <br>
    <br>
    Task Name :{task_name} Failed.<br>
    <br>
    """.format(**contextDict)

    send_email('youremail@gmail.com', title, body)


default_args = {
   'email': 'youremail@gmail.com',
   'email_on_failure': True,
}


with DAG('data-migration-pipeline',
         default_args=default_args,
         start_date=datetime(2022, 1, 1),
         schedule_interval="@once") as dag:

    t1 = PythonOperator(
        task_id='GCSToLocalDownload',
        python_callable=download_file,
        email_on_failure=True,
        on_failure_callback=notify_email,
        dag=dag)

    t2 = PythonOperator(
        task_id='ExcelImportToCSV',
        python_callable=excelToCSV,
        depends_on_past=True,
        email_on_failure=True,
        on_failure_callback=notify_email,
        dag=dag)

    t3 = PythonOperator(
        task_id='ApplyTransformation',
        python_callable=transformation,
        depends_on_past=True,
        email_on_failure=True,
        on_failure_callback=notify_email,
        dag=dag)

    t4 = PythonOperator(
        task_id='LocalToGCSUpload',
        python_callable=processed,
        depends_on_past=True,
        email_on_failure=True,
        on_failure_callback=notify_email,
        dag=dag)

    t5 = EmailOperator(
        task_id="SendStatusEmail",
        to='youremail@gmail.com',
        subject='Data Migration Pipeline Status!',
        html_content='<p>Hi Everyone, Migration Process completed Successfully! <p>',
        dag=dag)


t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)

