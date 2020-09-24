from flask import Flask
from flask_jwt_extended import JWTManager
from flask_cors import CORS

from config import Development, UserAcceptanceTest
from db import db #, nosqldb

from blue_prints.user import  user_bp
from blue_prints.data_load import data_bp
from blue_prints.retail import retail_bp
from blue_prints.retail_chatbot import retail_chatbot_bp
from blue_prints.cash_flow import cash_flow_bp

jwt = JWTManager()

def create_app():

    app = Flask(__name__) 
    
    # Load config
    if app.config['ENV'] == "uat":
        config = UserAcceptanceTest()
    else:        
        config = Development()
   
    
    app.config.from_object(config)
    
    print('mysql database name', config.MYSQL_DB)    
    @app.before_first_request
    def create_tables():
        db.create_all() 
        
    # register extensions like db, jwt, cors      
    extensions(app)
   
    
    #add restful resources   
    # api = Api(app)        
    add_resource(app)
    
    return app

def extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None    """    
    db.init_app(app)   
    jwt.init_app(app)  
    CORS(app) #https://github.com/corydolphin/flask-cors
    # CORS(app, resources={r"*": {"origins": "*"}})
    
    return None


def add_resource(app):
    """
    Add resources 
    
    :param app: Flask restful api instance
    :return: None    """
    app.register_blueprint(user_bp)
    app.register_blueprint(data_bp)
    app.register_blueprint(retail_bp)
    app.register_blueprint(retail_chatbot_bp)
    app.register_blueprint(cash_flow_bp)

    return None
