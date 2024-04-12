# Import necessary packages
import requests
import time
from datetime import datetime
from mastodon import Mastodon
from dotenv import load_dotenv
import os
import re
import json
from hdfs import InsecureClient

load_dotenv()

# Create an instance of mastodon
mastodon = Mastodon(
    client_id=os.getenv('client_id'),
    client_secret=os.getenv('client_secret'),
    access_token=os.getenv('access_token'),
    api_base_url='https://mastodon.social'
)

my_account = mastodon.account_verify_credentials()

# Define parameters
params = {
    'limit': 40
}

# Request the api
response = requests.get('https://mastodon.social/api/v1/timelines/public', params)

mastodon_data = []
if (response.status_code) == 200:
    actual_data = response.json()
mastodon_data.extend(actual_data)

list_links = []

# Establish a connection to HDFS
client = InsecureClient('http://namenode:9870', user='like')

while(True):

    # Retrieve next link
    next_link = re.search(r'<(.*?)>; rel="next"', response.headers['link']).group(1)

    if next_link in list_links:
        break

    list_links.append(next_link)

    try:
        # Fetch next link
        response = requests.get(next_link, params)

        # Process if status is 200 and get the new data
        if (response.status_code) == 200:
            actual_data = response.json()
        mastodon_data.extend(actual_data)

        # Print the current length
        print(f"The len of the data is {len(mastodon_data)}", end='\r')

        # Break if len reached
        if len(mastodon_data) > 100:
            break
    except: 
        # Break in errors occurs
        print('Failed to fetch next link')
        break


# Transform data into a JSON file
file_path = '../Data/mastodon_data.json'
with open(file_path, 'w') as file:
    json.dump(mastodon_data, file)

cuurent_datetime = datetime.now().strftime("%Y-%m-%d_%H_%M")

# Load the JSON file into HDFS
client.upload(f"/hadoop/data/mastodon/raw/mastodon_data{cuurent_datetime}.json", file_path)

# End of process
print('End of process')