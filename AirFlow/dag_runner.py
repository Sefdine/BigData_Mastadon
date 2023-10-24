from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'engagement_script_dag',
    default_args=default_args,
    description='A DAG to run the engagement script',
    schedule_interval=timedelta(days=1),
)

run_script = BashOperator(
    task_id='run_engagement_script',
    bash_command='python3 ~/airflow/BigDATA_Mastodon/MapReduce/main.py ~/airflow/BigDATA_Mastodon/Data/mastodon_data.json',
    dag=dag,
)

run_script
