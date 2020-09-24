from flask_restful import Resource, reqparse
from werkzeug.security import safe_str_cmp
from flask_jwt_extended import create_access_token
from datetime import datetime, timedelta

from models.user import AAP_USERS


class UserRegister(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('username',
                        type=str,
                        required=True,
                        help="This field cannot be blank."
                        )
    parser.add_argument('password',
                        type=str,
                        required=True,
                        help="This field cannot be blank."
                        )
    parser.add_argument('email',
                        type=str,
                        required=True,
                        help="This field cannot be blank."
                        )

    def post(self):
        print ("here")
        data = UserRegister.parser.parse_args()

        if AAP_USERS.find_by_email(data['email']):        
            return {"message": "A user with that username already exists"}, 400

        AAP_USERS.save_to_db(data['username'], data['password'],data["email"] )        
        return {"message": "User created successfully."}, 201



class UserAuthorization(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('email',
                        type=str,
                        required=True,
                        help="This field cannot be blank."
                        )
    parser.add_argument('password',
                        type=str,
                        required=True,
                        help="This field cannot be blank."
                        )
    

    def post(self):
        print ("here")
        data = UserAuthorization.parser.parse_args()
        if not "email" in data or not "password" in data:
            error = {
                "code": "MISSING_username_OR_PASSWORD"
            }
            return {'error': error}, 400
        

        user = AAP_USERS.find_by_email(data['email'])        
        
        if not user or not safe_str_cmp(user["password"], data["password"]):
            error = {
                "code": "INCORRECT_CREDENTIALS"
            }
            return {'error': error}, 403
        
        if user and safe_str_cmp(user["password"], data["password"]):
            print("validated in auth")

        expires = timedelta(hours=24)
        access_token = create_access_token(identity=user["email"], expires_delta=expires)
        
        return {"access_token":access_token}, 200

