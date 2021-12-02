from flask import Flask, Response, request, url_for, redirect, g
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decouple import config
import json
import uuid
import time
from decimal import Decimal

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")
REGION_NAME = config("REGION_NAME")

app = Flask(__name__)

# -------------------- authentication --------------------
import os
import re
import requests
from flask_cors import CORS
from flask_dance.contrib.google import google, make_google_blueprint
from context import get_google_blueprint_info, API_GATEWAY_URL

CORS(app)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'
app.secret_key = "e6156"
google_blueprint_info = get_google_blueprint_info()
google_blueprint = make_google_blueprint(
    client_id = google_blueprint_info["client_id"],
    client_secret = google_blueprint_info["client_secret"],
    scope = ["profile", "email"]
)
app.register_blueprint(google_blueprint, url_prefix="/login")
google_blueprint = app.blueprints.get("google")

paths_do_not_require_security = [
    '/login/google/?.*'
]

@app.before_request
def before_request():
    for regex in paths_do_not_require_security:
        if re.match(regex, request.path):
            return

    if not google.authorized:
        return redirect(url_for('google.login'))
    
    try:
        # print(json.dumps(google_blueprint.session.token, indent=2))
        user_data = google.get('/oauth2/v2/userinfo').json()
        email = user_data['email']
        url = f"{API_GATEWAY_URL}/api/users?email={email}"
        cookies = request.cookies
        response = requests.get(url, cookies=cookies)
        result = response.json()

        if len(result) == 0:
            url = f"{API_GATEWAY_URL}/api/users"
            user_id = str(uuid.uuid4())
            template = {
                'user_id': user_id,
                'first_name': user_data['given_name'],
                'last_name': user_data['family_name'],
                'nickname': user_data['email'],
                'email': user_data['email'],
            }
            response = requests.post(url, data=template, cookies=cookies)
        else:
            user_id = result[0]['user_id']
        g.user_id = user_id
        g.email = email
    except:
        # for oauthlib.oauth2.rfc6749.errors.TokenExpiredError
        return redirect(url_for('google.login'))

resource = boto3.resource(
    'dynamodb',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)
table = resource.Table('Posts')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return json.JSONEncoder.default(self, obj)


@app.route('/api/posts', methods=['GET'])
def get_posts():
    search = request.args.get('search')
    if search is None:
        scan_kwargs = {
            'ProjectionExpression': "post_id, title, user_id, update_time",
        }
    else:
        search = search.lower()
        scan_kwargs = {
            'FilterExpression': Attr('search_title').contains(search),
            'ProjectionExpression': "post_id, title, user_id, update_time",
        }
    # Todo: pagination?
    response = table.scan(**scan_kwargs)
    rsp = Response(json.dumps(response['Items'], cls=DecimalEncoder), status=200, content_type="application/json")
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
    if 'post_id' not in data:
        data['post_id'] = str(uuid.uuid4())
    data['version_id'] = str(uuid.uuid4())
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data['create_time'] = create_time
    data['update_time'] = create_time
    data['search_title'] = data['title'].lower()
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
        rsp = Response(json.dumps(data, default=str), status=201, content_type="application/json")
    except Exception as e:
        rsp = Response("{}".format(e), status=400)

    return rsp


@app.route('/api/posts/<post_id>', methods=['GET'])
def get_post_by_id(post_id):
    response = table.query(
        KeyConditionExpression=Key('post_id').eq(post_id)
    )
    rsp = Response(json.dumps(response['Items'], default=str), status=200, content_type="application/json")
    return rsp


@app.route('/api/posts/<post_id>', methods=['PUT'])
def update_post(post_id):
    data = request.get_json()
    new_tile = data['title']
    search_title = data['title'].lower()
    new_content = data['content']
    update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    # resolve write-write conflict
    old_version_id = data['version_id']
    new_version_id = str(uuid.uuid4())

    try:
        response = table.update_item(
            Key={
                'post_id': post_id,
            },
            UpdateExpression="set title = :t, search_title = :s, content = :c, update_time = :u, version_id = :nvid",
            ConditionExpression="version_id = :ovid",
            ExpressionAttributeValues={
                ':ovid': old_version_id,
                ':nvid': new_version_id,
                ':c': new_content,
                ':t': new_tile,
                ':s': search_title,
                ':u': update_time
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        rsp = Response("{}".format(e), status=400)
        return rsp
    else:
        rsp = Response(json.dumps(response['Attributes'], default=str), status=200, content_type="application/json")
        return rsp


@app.route('/api/posts/<post_id>/comments', methods=['POST'])
def create_comment(post_id):
    data = request.get_json()
    data['comment_id'] = str(uuid.uuid4())
    data['version_id'] = str(uuid.uuid4())
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data['create_time'] = create_time
    data['update_time'] = create_time
    try:
        # Condition: avoid duplicated partition key
        response = table.update_item(
            Key={
                'post_id': post_id
            },
            UpdateExpression="SET comments = list_append(if_not_exists(comments, :empty_list), :c)",
            ExpressionAttributeValues={
                ':c': [data],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )
        rsp = Response(json.dumps(data, default=str), status=201, content_type="application/json")
    except Exception as e:
        rsp = Response("{}".format(e), status=400)

    return rsp


@app.route('/api/posts/<post_id>/comments/<comment_index>', methods=['POST'])
def create_response(post_id, comment_index):
    data = request.get_json()
    data['response_id'] = str(uuid.uuid4())
    data['version_id'] = str(uuid.uuid4())
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data['create_time'] = create_time
    data['update_time'] = create_time
    try:
        # Condition: avoid duplicated partition key
        response = table.update_item(
            Key={
                'post_id': post_id
            },
            UpdateExpression="SET comments[" + comment_index + "].responses = "
                             "list_append(if_not_exists(comments[" + comment_index + "].responses, "
                             ":empty_list), :c)",
            ExpressionAttributeValues={
                ':c': [data],
                ':empty_list': [],
            },
            ReturnValues="UPDATED_NEW"
        )
        rsp = Response(json.dumps(data, default=str), status=201, content_type="application/json")
    except Exception as e:
        rsp = Response("{}".format(e), status=400)

    return rsp


@app.route('/')
def hello_world():  # put application's code here
    return f'Hello\n {g.email}\n {g.user_id}'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
