import requests
import datetime
import json
import pandas as pd
import sqlalchemy
import sqlite3

# APOD stands for Astronomy Picture Of the Day
APOD_URL = "https://api.nasa.gov/planetary/apod"
API_KEY = ""                                         #<--- Get your API Key from https://api.nasa.gov/ and enter it here
DB_LOCATION = "sqlite:///APOD_archive.sqlite"

# Helper function
def check_if_valid(df):
    if df.empty:
        raise Exception('No data recieved!')
    
    if df.isnull().values.any():
        raise Exception("Null values Found!")

# Fetching data from the API
print('Extracting Data...')
date = datetime.date.today()
params = {
    'api_key': API_KEY,
    'date': 'date',
    'hd': True
}

response = requests.get(APOD_URL, params=params)
if response.status_code != 200:
    print('Error retrieving data!')
    print(response.json()['msg'])
    exit()

response = response.json()
print('\nData Extracted:\n')
print(json.dumps(response, indent = 4))

result = {
    'date': response['date'],
    'title': response['title'],
    'media_type': response['media_type'],
    'url': response['hdurl'] if 'hdurl' in response else response['url'],
    'description': response['explanation'],
    'copyright': 'None' if 'copyright' not in response else response['copyright']
}

# Validating Data
print('\nValidating Data...')
check_if_valid(pd.DataFrame([result]))

# Downloading the Image
if result['media_type'] == 'image':
    print('\nDownloading Image...')
    image_filename = 'Image.jpg'
    response = requests.get(result['url'])
    if response.status_code != 200:
        print('Image download failed!')
        exit()

    with open(image_filename, 'wb') as f:
        f.write(response.content)

# Storing the data in database
print("\nLoading Data in the Database...")
engine = sqlalchemy.create_engine(DB_LOCATION)
conn = sqlite3.connect('APOD_archive.sqlite')
cursor = conn.cursor()

sql_create_query = """
    CREATE TABLE IF NOT EXISTS apod_archive(
        date TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        url TEXT,
        copyright TEXT,
        content BLOB
    )
"""
cursor.execute(sql_create_query)
    
sql_insert_image_query = "INSERT INTO apod_archive VALUES (?, ?, ?, ?, ?, ?)"
sql_insert_video_query = '''\
    INSERT INTO apod_archive (
        date, 
        title, 
        description, 
        url, 
        copyright
    ) VALUES (?, ?, ?, ?, ?)
'''
try:
    if result['media_type'] == 'image':
        with open(image_filename, 'rb') as f:
            image = f.read()
        
        cursor.execute(
            sql_insert_image_query, 
            (
                result['date'], 
                result['title'], 
                result['description'], 
                result['url'], 
                result['copyright'],
                image
            )
        )
    else:
        cursor.execute(
            sql_insert_video_query, 
            (
                result['date'], 
                result['title'], 
                result['description'], 
                result['url'], 
                result['copyright']
            )
        )
except:
    raise Exception('Loading data into database failed!')
conn.commit()
print("\nData Stored in the Database\n")

conn.close()