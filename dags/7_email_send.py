#Importing Libraries:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator

# These args will get passed on to the python operator
default_args = {
    'owner': 'Akash',
    'start_date': datetime(2022, 3, 14),
    'email': ['akashsjce8050@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



# define the DAG
dag = DAG(
    '7_Email_operator',
    default_args=default_args,
    description='How to use Email Operator?',
    schedule_interval=timedelta(days=1),
    catchup=False
)
# define the task 

start = DummyOperator(
        task_id='start',
        dag=dag
    )
    
Email_fun = EmailOperator(
    task_id ='Email_fun',
    to = 'vijay.viju251196@gmail.com',
    subject =' Airflow',
    html_content ="""<h3> Test mail from airflow </h3>""",
    dag=dag
)

end = DummyOperator(
        task_id='end',
        dag=dag
    )

start >> Email_fun >> end

