from dotenv import load_dotenv
import os

load_dotenv()

API_GATEWAY_URL=os.getenv('API_GATEWAY_URL')

def get_db_info():
    '''
        get MySQL connection info from environment variables
    '''
    
    db_info = {
        "host": os.getenv("DBHOST"),
        "user": os.getenv("DBUSER"),
        "password": os.getenv("DBPASSWORD"),
    }

    return db_info

def get_google_blueprint_info():

    google_blueprint_info = {
        "client_id": os.getenv("GBPCLIENTID"),
        "client_secret": os.getenv("GBPCLIENTSECRET"),
    }
    
    return google_blueprint_info

def get_aws_access_key_info():

    aws_access_key_info = {
        "region_name": os.getenv("AWSREGIONNAME"),
        "aws_access_key_id": os.getenv("AWSACCESSKEYID"),
        "aws_secret_access_key": os.getenv("AWSACCESSKEYSECRET")
    }

    return aws_access_key_info