import  os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from pymongo import MongoClient
#from flask_pymongo import PyMongo


from config import  Development, UserAcceptanceTest


db = SQLAlchemy()


if os.getenv("FLASK_ENV") == "uat":
    config = UserAcceptanceTest()
    mongo_db = config.MONGO_DB
else:
    config = Development()
    mongo_db = config.MONGO_DB

#pymongo    
client = MongoClient(config.MONGO_URI)
nosqldb= client[mongo_db]
print('mongo database name',mongo_db)
#print(config.MONGO_URI)

# flask_pymongo
# app = Flask(__name__)
# if app.config["ENV"] == "uat":
#     config = UserAcceptanceTest()
#     mongo_db = config.MONGO_UAT_DB
# else:
#     config = Development()
#     mongo_db = config.MONGO_DEV_DB
# app.config.from_object(config)
# mongo = PyMongo(app)
# nosqldb = mongo.db

