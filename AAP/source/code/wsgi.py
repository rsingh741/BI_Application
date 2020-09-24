import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname('__file__'), '..')))

from app import create_app


app = create_app()

if __name__ == "__main__":
	host= os.getenv('APP_IP')
	port = os.getenv('APP_PORT')
	app.run(host=host, port=port)