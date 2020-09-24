       
# from db import  nosqldb



# class AAP_USERS:
#     print ('in')


#     user_collection = nosqldb.users

#     def __init__(self, id, username, password):
#         self.id = id
#         self.username = username
#         self.password = password

#     @classmethod
#     def save_to_db(cls,username,password,id):
#         cls.user_collection.insert({"username": username, "password": password, "id": id})

     
#     @classmethod
#     def find_by_username(cls, username):
#         user = cls.user_collection.find_one({"username": username})

#         return user

#     @classmethod
#     def find_by_id(cls, _id):
     
#         return cls.user_collection.find_one({"id": _id})
      
        
        
from db import nosqldb
#from app import nosqldb



class AAP_USERS:
    print ('in')   
    #print(nosqldb)

    user_collection = nosqldb.users

    def __init__(self, email, username, password):
        self.email = email
        self.username = username
        self.password = password

    @classmethod
    def save_to_db(cls,username,password,email):
        cls.user_collection.insert({"username": username, "password": password, "email": email})
        
     
    @classmethod
    def find_by_username(cls, username):
        user = cls.user_collection.find_one({"username": username})        
        return user

    @classmethod
    def find_by_email(cls, _email):
     
        return cls.user_collection.find_one({"email": _email})
        
        
