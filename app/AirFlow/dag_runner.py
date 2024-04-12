from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'engagement_script_dag',
    default_args=default_args,
    description='A DAG to run the engagement script',
    schedule_interval=timedelta(days=1),
)

# ********** User Engagement ************

user_engagement = BashOperator(
    task_id='user_engagement',
    bash_command='python3 /home/like/app/MapReduce/UserEngagement/engagement.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

user_followers = BashOperator(
    task_id='user_followers',
    bash_command='python3 /home/like/app/MapReduce/UserEngagement/followers.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

user_most_used_tag = BashOperator(
    task_id='user_most_used_tag',
    bash_command='python3 /home/like/app/MapReduce/UserEngagement/mostUsedTag.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

user_growth_over_time = BashOperator(
    task_id='user_growth_over_time',
    bash_command='python3 /home/like/app/MapReduce/UserEngagement/userGrowthOverTime.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

# *********** Content Popularity **************

content_language_script = BashOperator(
    task_id='content_language_script',
    bash_command='python3 /home/like/app/MapReduce/ContentPopularity/languagePostCategorize.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

content_multimedia_script = BashOperator(
    task_id='content_multimedia_script',
    bash_command='python3 /home/like/app/MapReduce/ContentPopularity/postMultimediaCount.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

content_shared_website = BashOperator(
    task_id='content_shared_website',
    bash_command='python3 /home/like/app/MapReduce/ContentPopularity/sharedWebsite.py /home/like/app/Data/mastodon_data.json',
    dag=dag,
)

user_engagement >> user_followers >> user_most_used_tag >> user_growth_over_time >> content_language_script >> content_multimedia_script >> content_shared_website
