# Import the libraries
from datetime import timedelta,datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import os


default_args = {
    'owner': 'shaheer',  
    'start_date': datetime.today(),  
    'email': ['dummy@example.com'], 
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /usr/local/airflow/dags/finalassignment/tolldata.tgz -C /usr/local/airflow/dags/finalassignment/ ',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /usr/local/airflow/dags/finalassignment/vehicle-data.csv >  \
        /usr/local/airflow/dags/finalassignment/csv_data.csv ',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d"\t" -f5-7 /usr/local/airflow/dags/finalassignment/tollplaza-data.tsv   \
        | tr "\t" "," > /usr/local/airflow/dags/finalassignment/tsv_data.csv ',
    dag=dag,
)


extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='paste -d"," \
        <(cut -c 59-61 /usr/local/airflow/dags/finalassignment/payment-data.txt) \
        <(cut -c 63-67 /usr/local/airflow/dags/finalassignment/payment-data.txt) \
        > /usr/local/airflow/dags/finalassignment/fixed_width_data.csv ',
    dag=dag,        
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," \
        <(cut -d"," -f1- /usr/local/airflow/dags/finalassignment/csv_data.csv) \
        <(cut -d"," -f1- /usr/local/airflow/dags/finalassignment/tsv_data.csv) \
        <(cut -d"," -f1- /usr/local/airflow/dags/finalassignment/fixed_width_data.csv) \
        > /usr/local/airflow/dags/finalassignment/consolidated_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr a-z A-Z < /usr/local/airflow/dags/finalassignment/consolidated_data.csv \
        > /usr/local/airflow/dags/finalassignment/staging/transformed_data.csv ',
    dag=dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
