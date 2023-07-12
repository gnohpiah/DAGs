from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#Task 1.1
default_args = {
    'owner': 'Phong',
    'start_date': days_ago(0),
    'email': ['test@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

#Task 1.2
dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1)
)

#Task 1.3
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz  -C  /home/project/airflow/dags/finalassignment',
    dag = dag,
)

#Task 1.4
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = "cut -d ',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag = dag,
)

#Task 1.5
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = "cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr '[:blank:]' ',' > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag = dag,
)

#Task 1.6
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = "cut -c 59-62,63-67 /home/project/airflow/dags/finalassignment/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag = dag,
)

#Task 1.7
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv | tr -d "\r" > extracted_data.csv',
    dag = dag,
)

#Task 1.8
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = "awk -F ',' '{$4 = toupper($4);print}' extracted_data.csv > transformed_data.csv",
    dag = dag,
)

#Task 1.9
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data