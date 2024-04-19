#import
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#define DAG arguments
default_args = {
    'owner': 'Zaini',
    'start_date': days_ago(0),
    'email': ['grzaini@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#define the unzip task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command = "tar -xvzf /home/zaini/airflow/dags/finalassignment/tolldata.tgz -C /home/zaini/airflow/dags/finalassignment/",
    dag=dag
)

#define the extraction tasks
extract_data_from_csv = BashOperator(
    task_id = "extract_data_from_csv",
    bash_command = "cut -d, -f1,2,3,4 /home/zaini/airflow/dags/finalassignment/vehicle-data.csv > /home/zaini/airflow/dags/finalassignment/csv_data.csv",
    dag = dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 /home/zaini/airflow/dags/finalassignment/tollplaza-data.tsv | tr -s "\t", "," > /home/zaini/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59-67 /home/zaini/airflow/dags/finalassignment/payment-data.txt | tr -s "[:blank:]", "," > /home/zaini/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag
)

#define the consolidate_data task
csv_file = '/home/zaini/airflow/dags/finalassignment/csv_data.csv'
tsv_file = '/home/zaini/airflow/dags/finalassignment/tsv_data.csv'
fixed_width_file = '/home/zaini/airflow/dags/finalassignment/fixed_width_data.csv'
extracted_data = '/home/zaini/airflow/dags/finalassignment/extracted_data.csv'
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste -d"," {csv_file} {tsv_file} {fixed_width_file} > {extracted_data}',
    dag=dag
)

#define transform task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk 'BEGIN {FS=OFS\",\"} { $4= toupper($4)} 1' /home/zaini/airflow/dags/finalassignment/extracted_data.csv > /home/zaini/airflow/dags/finalassignment/transformed_data.csv",
    dag=dag
)

#task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


