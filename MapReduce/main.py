import subprocess
import sys
import os

# Get absolute paths
home_directory = os.path.expanduser("~")
data_file = os.path.abspath(sys.argv[1])

# ********** User Engagement ************
user_engagement = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/UserEngagement/engagement.py")
user_followers = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/UserEngagement/followers.py")
user_most_used_tag = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/UserEngagement/mostUsedTag.py")
user_growth_over_time = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/UserEngagement/userGrowthOverTime.py")

# ********** Content popularity ************
content_language_script = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/ContentPopularity/languagePostCategorize.py")
content_multimedia_script = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/ContentPopularity/postMultimediaCount.py")
content_shared_website = os.path.join(home_directory, "airflow/BigDATA_Mastodon/MapReduce/ContentPopularity/sharedWebsite.py")

try:
    print('\n*********** User Engagement **************\n')
    print('\n++++++++++++++ Calculating user engagement rates +++++++++++++++\n')
    subprocess.run(["python3", user_engagement, data_file], check=True)
    print('\n+++++++++ Identifying users with the highest number of followers +++++++++\n')
    subprocess.run(["python3", user_followers, data_file], check=True)
    print('\n+++++++ Identifying users mentioned in the most used tags +++++++\n')
    subprocess.run(["python3", user_most_used_tag, data_file], check=True)
    print('\n+++++++ Studying user growth over time +++++++\n')
    subprocess.run(["python3", user_growth_over_time, data_file], check=True)

    print('\n*********** Content Popularity **************\n')
    print('\n++++++++++++++ Categorize posts by languages +++++++++++++++\n')
    subprocess.run(["python3", content_language_script, data_file], check=True)
    print('\n++++++++++++++ How many content have multimedia attached ? +++++++++++++++\n')
    subprocess.run(["python3", content_multimedia_script, data_file], check=True)
    print('\n++++++++++++++ Analyze shared website +++++++++++++++\n')
    subprocess.run(["python3", content_shared_website, data_file], check=True)

except subprocess.CalledProcessError as e:
    print(f"An error occurred: {e}")

