from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import vertica_python
from getpass import getpass

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2022, 10, 25),
   'retries': 0,
   'retry_delay': dt.timedelta(minutes=1),
    'catchup': False,
}

dag = DAG (
    dag_id='data_to_stg.py',
	# в 15 минут следующего дня, через 10 мин после S3_to_data
    schedule_interval="15 0 * * *",
    default_args=args
)

def data_to_stg() -> None:
         
    conn_info = {'host': Variable.get("host"), 
                'port': Variable.get("port"),
                'user': Variable.get("user"),       
                'password': Variable.get("password"),
                'database': Variable.get("database"),
                'autocommit': Variable.get("autocommit")
    }

	#возьмем папку за вчерашнюю дату
	folder = datetime.today().date() - timedelta(days=1)
	
	with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()
        insert_stmt = f'COPY GRIGORJEVDEYANDEXRU__STAGING.currencies (date_update,currency_code,currency_code_with,currency_with_div) FROM LOCAL \'\data\\{str(folder)}\\currencies_history.csv\' DELIMITER \',\';'
		curs.execute(insert_stmt)	
		conn.commit()
	
	#пройдем в цикле по всем файлам из вчерашней папки
	for i in range(10):	
		with vertica_python.connect(**conn_info) as conn:
			curs = conn.cursor()
			insert_stmt = f'COPY GRIGORJEVDEYANDEXRU__STAGING.transactions (operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt) FROM LOCAL \'\data\\{str(folder)}\\transactions_batch_{i}.csv\' DELIMITER \',\';'
			curs.execute(insert_stmt)			
			conn.commit()
			
	#и запишем отсечку по дате для инкрементного наполнения витрины
	with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()
        insert_stmt = f'insert into GRIGORJEVDEYANDEXRU__STAGING.transactions_max_date (max_dt) values ({folder})'
		curs.execute(insert_stmt)	
		conn.commit()
		
data_to_stg = PythonOperator(task_id='data_to_stg',
                            python_callable=data_to_stg,
                            dag=dag)

data_to_stg