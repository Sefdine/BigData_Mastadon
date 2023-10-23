import subprocess
import sys

if len(sys.argv) != 2:
    print("Usage: python3 main.py ../Data/mastodon_data.json")
    sys.exit(1)

data_file = sys.argv[1]

# ********** User Engagement ************
user_engagement = "UserEngagement/engagement.py"
user_followers = "UserEngagement/followers.py"
user_most_used_tag = "UserEngagement/mostUsedTag.py"
user_growth_over_time = "UserEngagement/userGrowthOverTime.py"

# ********** Content popularity ************
content_language_script = "ContentPopularity/languagePostCategorize.py"
content_multimedia_script = "ContentPopularity/postMultimediaCount.py"
content_shared_website = "ContentPopularity/sharedWebsite.py"

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
