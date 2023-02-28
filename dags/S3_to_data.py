from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2022, 10, 01),
   'retries': 0,
   'retry_delay': dt.timedelta(minutes=1),
    'catchup': False,
}

dag = DAG (
    dag_id='S3_to_data.py',
	# в 5 минут следующего дня
    schedule_interval="5 0 * * *",
    default_args=args
)

def S3_to_data() -> None:
	
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID") # это сохранено в Airflow
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
    
	session = boto3.session.Session()
    s3_client = session.client(
        service_name='conn_S3',
        endpoint_url='https://storage.yandexcloud.net/final-project',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
	
	#создадим папку на вчерашнюю дату
	folder = datetime.today().date() - timedelta(days=1)
	os.mkdir(f'/data/{folder}')
	
	#загрузим файлы за вчера 
    s3_client.download_file(
        Bucket='final-project',
        Key='currencies_history.csv',
        Filename=f'/data/{str(folder)}/currencies_history.csv'
    )        
	for i in range(10):
		s3_client.download_file(
        Bucket='final-project',
        Key=f'transactions_batch_{i}.csv',
        Filename=f'/data/{str(folder)}/transactions_batch_{i}.csv'
		) 
		
S3_to_data = PythonOperator(task_id='S3_to_data',
                            python_callable=S3_to_data,
                            dag=dag)

S3_to_data