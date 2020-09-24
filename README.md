# BI_Application
python backend 



## python version
python 3.7 

## Installing the Components from the Ubuntu 
* sudo apt update
* sudo apt install python3.7-pip python3.7-dev build-essential libssl-dev libffi-dev python3.7-setuptools
* sudo apt install libpq-dev postgresql-dev 
requirements_DS.txt
## Creating a Python Virtual Environment
* sudo apt install python3-venv
* cd ../analyticcal_platform/AAP/
* python3.7 -m venv myprojectenv
* source venv/bin/activate
* pip install -r Requirement-API-services.txt
* pip install -r requirements_DS.txt

# running flaks app server
### run following file inside /AAP/source/code
uwsgi --socket 0.0.0.0:5020 --protocol=http -w wsgi:app


