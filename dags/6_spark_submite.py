#Importing Libraries:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
#from airflow.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# These args will get passed on to the python operator
default_args = {
    'owner': 'Akash',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 5),
    'email': ['akashsjce8050@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# define the DAG
dag = DAG(
    '6_Python_script_spark_submite',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1)
)
# define the task 

start = DummyOperator(
        task_id='start',
        dag=dag
    )



spark_submit_local = SparkSubmitOperator(
		application ='/home/saif/PycharmProjects/cohort_c9/airflow_with_textfile_df_mysql_airflow.py"' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)
		
end = DummyOperator(
        task_id='end',
        dag=dag,
        depends_on_past= True,
    )

start >> spark_submit_local >> end



