from flask import Flask, Response, request, redirect, url_for
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Key
from decouple import config
import json
import uuid
import time

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")
REGION_NAME = config("REGION_NAME")

app = Flask(__name__)
CORS(app)

resource = boto3.resource(
    'dynamodb',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
table = resource.Table('Posts')


@app.route('/api/posts', methods=['GET'])
def get_posts():
    scan_kwargs = {
        'ProjectionExpression': "post_id, title, user_id, update_time",
    }
    # Todo: how to do pagination?
    response = table.scan(**scan_kwargs)
    rsp = Response(json.dumps(response['Items'], default=str), status=200, content_type="application/json")
    return rsp

    # done = False
    # start_key = None
    # while not done:
    #     if start_key:
    #         scan_kwargs['ExclusiveStartKey'] = start_key
    #     response = table.scan(**scan_kwargs)
    #     display_movies(response.get('Items', []))
    #     start_key = response.get('LastEvaluatedKey', None)
    #     done = start_key is None


@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    data['post_id'] = '859ef704-1ef0-4992-b49c-11a8adf4152b'
    data['version_id'] = str(uuid.uuid4())
    create_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    data['create_time'] = create_time
    data['update_time'] = create_time
    try:
        # Condition: avoid duplicated partition key
        response = table.put_item(
            Item=data,
            Expected={
                'post_id': {
                    'Exists': False
                }
            }
        )
        rsp = Response(json.dumps(response, default=str), status=200, content_type="application/json")
    except Exception as e:
        rsp = Response("{}".format(e), status=500)

    return rsp


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
