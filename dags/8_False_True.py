from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.email_operator import EmailOperator 
from airflow.utils.email import send_email 
from datetime import timedelta, datetime 


def sucess(context): 
    dr=context.get("dag_run") 
    msg="<h3>** DAG run was successful**</h3>" 
    to='akashsjce8050@gmail.com' 
    subject="DAG Status" 
    send_email(to=to,subject=subject,html_content=msg) 
 

def failure(context): 
    dr=context.get("dag_run") 
    msg=f"<h3>** Failure**</h3> {dr}" 
    to='mahindrakar.akash@futurense.com' 
    subject="DAG Status" 
    send_email(to=to,subject=subject,html_content=msg) 

# These args will get passed on to the python operator 
default_args = { 
    'owner': 'athreya', 
    'depends_on_past': False, 
    'start_date': datetime(2022, 3, 10), 
    'email': ['akashsjce8050@gmail'], 
    'email_on_failure': True, 
    'email_on_retry': False, 
    'on_failure_callback':failure, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5) 
     
} 


# define the DAG 
dag = DAG( 
    'AP_sf_operator', 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=10), 
    catchup=False 
) 

start=DummyOperator( 
    task_id='start', 
    dag=dag 
) 

# define the first task 

bash1 = BashOperator( 
    task_id ='bash1', 
    bash_command="pwd1", 
    on_success_callback=sucess, 
    dag=dag 
) 

 
end=DummyOperator( 
    task_id='end', 
    dag=dag 
) 


start>>bash1>>end 

