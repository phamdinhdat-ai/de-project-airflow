from pymongo import MongoClient
from clickhouse_driver import Client
from datetime import datetime

import os
from os.path import join, dirname
from dotenv import load_dotenv


# file_path = join(dirname(__file__), ".env")
# load_dotenv(file_path)
# # Connect to MongoDB

print("connecting to mongodb client")
mongo_client = MongoClient('mongodb://' + os.environ['MONGO_USER'] + ':' + os.environ['MONGO_PASSWORD'] + '@' + os.environ['MONGO_HOST'] + ':27017/')
# mongo_client = MongoClient('mongodb://127.0.0.1:27017/')
mongo_db = mongo_client[os.environ['MONGO_DB']]
mongo_collection = mongo_db[os.environ['MONGO_COLLECTIONS']]
# mongo_collection.find()
print("mongo collection: ", [data for data in mongo_collection.find({'bedroom':3})])
print('mongo-accessed')
# Connect to ClickHouse

# Extract data from MongoDB
mongo_data = mongo_collection.find()


# print("len data mongo:", len(mongo_data.to_list()))
# Load data into ClickHouse
ch_client = Client(host=os.environ['CLICKHOUSE_HOST'], user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'])
ch_client.execute(F"CREATE DATABASE IF NOT EXISTS {os.environ['DATABASE']}")
ch_client.execute(F"USE {os.environ['DATABASE']}")

print("clickhouse - client accessed !")
# Delete the table in ClickHouse
ch_client.execute(f'''
    DROP TABLE IF EXISTS {os.environ['TABLE']}
''')

# Create a table in ClickHouse
ch_client.execute(f'''
    CREATE TABLE IF NOT EXISTS {os.environ['TABLE']} (
        id String,
        price Decimal(10,2),
        bedroom Int8,
        bathroom Int8,
        area Int32,
        district String,
        city String,
        release_date Date
    ) ENGINE = MergeTree()
    ORDER BY id
''')

# print("abdc")
for document in mongo_data:
    # print(document)
    release_date = datetime.strptime(document['releaseDate'], '%Y/%m/%d')
    # print("ix")
    ch_client.execute(f"INSERT INTO {os.environ['TABLE']} VALUES", [(
        str(document['id']),
        document['price'],
        document['bedroom'],
        document['bathroom'],
        document['area'],
        document['district'],
        document['city'],
        release_date
    )])
    # print("document : ", i)
    # with open("data_houses.txt", 'w') as f:
    #     f.writelines(document)
    
print(f"Write successfully data into table: {os.environ['TABLE']} - Database: {os.environ['DATABASE']}  ! huraaaaaaaaaaaaaaaa fuck!")

# os.environ['BATCH_NAME'] = "5509954488340082625301"
# ENV_CONFIG = "utils/env_config.ini"

# import boto3 

# def uploadDir(dir_path,prefix):
#     s3_client = boto3.client('s3')
#     if '/' not in dir_path:
#         dir_path += '/'
        
#     root = os.path.relpath(dir_path)
#     for path, subdirs, files in os.walk(root):
#         for name in files:
#             file_path=os.path.join(path, name)
#             s3_uri=os.path.join(prefix,path,name).replace(dir_path,'').replace('/..','/').replace('//','/').replace('..','')
#             print('s3_uri: ',s3_uri)
#             # UPLOAD FILE
#             s3_client.upload_file(file_path, os.environ['OUTPUT_BUCKET'], s3_uri)

