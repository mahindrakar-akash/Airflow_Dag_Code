#Importing Libraries:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

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
    '5_Python_script',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1)
)
# define the task 

start = DummyOperator(
        task_id='start',
        dag=dag
    )

operation1 = BashOperator(
    task_id ='operation1',
    bash_command = "spark-submit --jars /home/saif/LFS/cohort_c9/jars/mysql-connector-java-8.0.27.jar /home/saif/PycharmProjects/cohort_c9/airflow_with_textfile_df_mysql_airflow.py",
    dag=dag
    
)

end = DummyOperator(
        task_id='end',
        dag=dag,
        depends_on_past= True,
    )

start >> operation1 >> end



