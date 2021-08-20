import os
from pymongo import MongoClient

url = os.getenv('DB_URL')
usrname = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT"))

connection = MongoClient(url,port)
collection = connection["todo_app_db"]["rabbitmq_messages"]