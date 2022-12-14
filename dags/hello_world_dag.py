from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

args = {
    'depends_on_past': False,
    'retries': 0
}
 
def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('hello_world_tinkoff', description='Hello World DAG',
          schedule_interval=None,
          start_date=datetime(2022, 3, 20), 
          catchup=False,
          default_args=args
        )

with dag:        
    hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator