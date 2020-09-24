from dotenv import load_dotenv
import os
load_dotenv()

class Config(object):
    DEBUG = True   
    SECRET_KEY = "**********************************"    
    
    MYSQL_USER     = os.getenv('MYSQL_USER')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_URL      = os.getenv('MYSQL_URL')
    MYSQL_PORT     = os.getenv('MYSQL_PORT')
    #MYSQL_DEV_DB   = os.getenv('MYSQL_DEV_DB')
    #MYSQL_UAT_DB   = os.getenv('MYSQL_UAT_DB')
    
    SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 300,
    }
    SQLALCHEMY_POOL_SIZE = 100
    JWT_REQUIRED_CLAIMS = ['exp', 'iat']
    JWT_SECRET_KEY = '***********************************'
    JWT_HEADER_TYPE = 'jwt'    
    PROPAGATE_EXCEPTIONS = True
    
    MONGO_URL      = os.getenv('MONGO_URL')
    MONGO_PORT     = os.getenv('MONGO_PORT')
    MONGO_USER     = os.getenv('MONGO_USER')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
    #MONGO_DEV_DB   = os.getenv('MONGO_DEV_DB')
    #MONGO_UAT_DB   = os.getenv('MONGO_UAT_DB')
    
class Development(Config):
    cg = Config()
    DEBUG = True
    MYSQL_DB   = os.getenv('MYSQL_DEV_DB')
    MONGO_DB   = os.getenv('MONGO_DEV_DB')
    SQLALCHEMY_DATABASE_URI= f'mysql+pymysql://{cg.MYSQL_USER}:{cg.MYSQL_PASSWORD}@{cg.MYSQL_URL}:{ cg.MYSQL_PORT}/{MYSQL_DB}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False    
    MONGO_URI = f"mongodb://{cg.MONGO_USER}:{cg.MONGO_PASSWORD}@{cg.MONGO_URL}:{cg.MONGO_PORT}/{MONGO_DB}?authSource={MONGO_DB}"
    
class UserAcceptanceTest(Config):
    FLASK_ENV = 'testing'
    cg = Config()
    DEBUG = False
    MYSQL_DB   = os.getenv('MYSQL_UAT_DB')
    MONGO_DB   = os.getenv('MONGO_UAT_DB')
    SQLALCHEMY_DATABASE_URI= f'mysql+pymysql://{cg.MYSQL_USER}:{cg.MYSQL_PASSWORD}@{cg.MYSQL_URL}:{ cg.MYSQL_PORT}/{MYSQL_DB}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False  
    MONGO_URI = f"mongodb://{cg.MONGO_USER}:{cg.MONGO_PASSWORD}@{cg.MONGO_URL}:{cg.MONGO_PORT}/{MONGO_DB}?authSource={MONGO_DB}"

class Local(Config):
    cg = Config()
    DEBUG=True
    MYSQL_DB   = os.getenv('MYSQL_DEV_DB')
    MONGO_DB   = os.getenv('MONGO_DEV_DB')
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:aroha@localhost:3306/dev_my360'
    SQLALCHEMY_TRACK_MODIFICATIONS = False    
    MONGO_URI = f"mongodb://{cg.MONGO_USER}:{cg.MONGO_PASSWORD}@{cg.MONGO_URL}:{cg.MONGO_PORT}/{MONGO_DB}?authSource={MONGO_DB}"
