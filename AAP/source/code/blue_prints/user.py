from flask import Blueprint
from flask_restful import Api

from resources.user import UserRegister, UserAuthorization

user_bp = Blueprint('user_api', __name__)
user_api = Api(user_bp)

user_api.add_resource(UserRegister, '/register')
user_api.add_resource(UserAuthorization, '/auth')  