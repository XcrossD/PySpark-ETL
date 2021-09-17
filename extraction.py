import requests
import json
import os
from pyspark.sql import SparkSession

def get_anime(url = None):
    if url == None:
        url = "https://kitsu.io/api/edge/anime?page[limit]=20&page[offset]=0"
    response = requests.get(url)
    return response

if __name__ == "__main__":
    path = 'json'
    if not os.path.isdir(path):
        os.mkdir(path)
    os.chdir(path)
    
    count = 1
    
    while count == 1 or next_link != None:
        response = get_anime()
        obj = response.json()
        with open(f'anime{count}.json', 'w') as file:
            json.dump(obj['data'], file)
        if 'next' in obj['links']:
            next_link = obj['links']['next']
        else:
            next_link = None
        last_link = obj['links']['last']
        count += 1