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
	# в 2 часа ночи следующего дня, чтобы с запасом
    schedule_interval="0 2 * * *",
    default_args=args
)


def stg_to_cdm() -> None:

    conn_info = {'host': Variable.get("host"), 
                'port': Variable.get("port"),
                'user': Variable.get("user"),       
                'password': Variable.get("password"),
                'database': Variable.get("database"),
                'autocommit': Variable.get("autocommit")
    }

	with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()
 		select_stmt = 'select max(transaction_dt)::date from GRIGORJEVDEYANDEXRU__STAGING.transactions t'
		
	with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()
 		insert_stmt = '''insert into GRIGORJEVDEYANDEXRU.global_metrics (date_update, 
																		currency_from, 
																		amount_total, 
																		cnt_transactions, 
																		avg_transactions_per_account, 
																		cnt_accounts_make_transactions)
						select t.transaction_dt::date as date_update,
								t.currency_code as currency_from,
								round(sum(t.amount/100),2) as amount_total,
								count(t.operation_id) as cnt_transactions,
								avg(t.amount*c.currency_with_div) as avg_transactions_per_account,
								COUNT(distinct t.account_number_from) as cnt_accounts_make_transactions 
						from GRIGORJEVDEYANDEXRU__STAGING.transactions t 
						join GRIGORJEVDEYANDEXRU__STAGING.currencies c on t.currency_code  = c.currency_code  
						where t.transaction_dt::date > (select max(max_dt)
														from GRIGORJEVDEYANDEXRU__STAGING.transactions_max_dt)
						group by t.transaction_dt::date,
								t.currency_code
						'''
		curs.execute()
		connection.commit()
		connection.close()
    
stg_to_cdm = PythonOperator(task_id='stg_to_cdm',
                            python_callable=stg_to_cdm,
                            dag=dag)

stg_to_cdm