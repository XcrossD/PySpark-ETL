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
    
    file_list = os.listdir()
    file_list.sort(key=len)
    if len(file_list) > 0:
        last_file = file_list[-1]
        count = last_file.split('.')[0][5:]
    else:
        count = 1
    
    next_link = None if count == 1 else f"https://kitsu.io/api/edge/anime?page[limit]=20&page[offset]={count * 20}"
    while count == 1 or next_link != None:
        response = get_anime(next_link)
        obj = response.json()
        with open(f'anime{count}.json', 'w') as file:
            json.dump(obj['data'], file)
        if 'next' in obj['links']:
            next_link = obj['links']['next']
        else:
            next_link = None
        # last_link = obj['links']['last']
        count += 1