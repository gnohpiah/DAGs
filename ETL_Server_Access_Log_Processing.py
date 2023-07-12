from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Phong',
    'start_date': days_ago(0),
    'email': ['test@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'ETL_Server_Access_Log_Processing',
    default_args = default_args,
    description = 'ETL Server Access Log Proccessing',
    schedule_interval = timedelta(days=1)
)

download = BashOperator(
    task_id = 'download',
    bash_command = 'wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt -O web-server-access-log.txt',
    dag = dag,
)

extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -d"#" -f1,4 web-server-access-log.txt > /home/project/airflow/dags/extracted-data1.txt',
    dag = dag,
)

transform = BashOperator(
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted-data1.txt > /home/project/airflow/dags/transformed-data1.txt',
    dag = dag,
)

load = BashOperator(
    task_id = 'load',
    bash_command = 'zip log.zip transformed-data1.txt',
    dag = dag,
)

download >> extract >> transform >> load