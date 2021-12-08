from operator import itemgetter
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
import requests
from flask_cors import CORS
from context import API_GATEWAY_URL
CORS(app)

@app.before_request
def before_request():
    # return
    # verify id_token
    id_token = request.headers.get('id_token')
    url = f"https://oauth2.googleapis.com/tokeninfo?id_token={id_token}"
    response = requests.get(url)
    user_data = response.json()
    email = user_data.get('email')

    # if not verified, return message
    if not email:
        response = Response("Please provide a valid google id_token!", status=200)
        return response

    # if verified
    url = f"{API_GATEWAY_URL}/api/users?email={email}"
    headers = {'id_token': id_token}
    response = requests.get(url, headers=headers)
    result = response.json()

    # check if user exist
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
        response = requests.post(url, data=template, headers=headers)
    else:
        user_id = result[0]['user_id']

    g.user_id = user_id
    g.email = email

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
    offset_s = request.args.get('offset')
    limit_s = request.args.get('limit')
    orderby_s = request.args.get('orderby')
    reverse_s = request.args.get('reverse')
    if search is None:
        scan_kwargs = {
            'ProjectionExpression': "post_id, title, user_id, update_time, last_comment_time",
        }
    else:
        search = search.lower()
        scan_kwargs = {
            'FilterExpression': Attr('search_title').contains(search),
            'ProjectionExpression': "post_id, title, user_id, update_time, last_comment_time",
        }
    # Todo: pagination?
    response = table.scan(**scan_kwargs)
    item = response['Items']

    # Bad: awkward pagination
    if orderby_s is None:
        orderby_s = "last_comment_time"
    if reverse_s is None:
        reverse = True
    else:
        reverse = bool(reverse_s)
    item.sort(key=itemgetter(orderby_s), reverse=reverse)
    if offset_s is None:
        offset = 0
    else:
        offset = int(offset_s)
    if limit_s is None:
        limit = len(item)
    else:
        limit = int(limit_s)
    end = min(len(item), offset + limit)
    return_item = item[offset:end]

    next_link = "/api/posts?"+\
                "offset="+str(offset+limit)+\
                "&limit="+str(limit)+\
                "&orderby="+orderby_s+\
                "&reverse="+str(reverse)
    self_link = "/api/posts?"+\
                "offset="+str(offset)+\
                "&limit="+str(limit)+\
                "&orderby="+orderby_s+\
                "&reverse="+str(reverse)
    prev_link = "/api/posts?"+\
                "offset="+str(max(offset-limit, 0))+\
                "&limit="+str(limit)+\
                "&orderby="+orderby_s+\
                "&reverse="+str(reverse)
    if search is not None:
        next_link += "&search="+search
        self_link += "&search="+search
        prev_link += "&search="+search

    result = {
        'data': return_item,
        'links': {
            'next':next_link,
            'self':self_link,
            'prev':prev_link
        }
    }
    rsp = Response(json.dumps(result, cls=DecimalEncoder), status=200, content_type="application/json")
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
    if 'user_id' not in data:
        data['user_id'] = g.user_id
    if 'post_id' not in data:
        data['post_id'] = str(uuid.uuid4())
    data['version_id'] = str(uuid.uuid4())
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    data['create_time'] = create_time
    data['update_time'] = create_time
    data['last_comment_time'] = create_time
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
    if 'user_id' not in data:
        data['user_id'] = g.user_id
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
            UpdateExpression="SET comments = list_append(if_not_exists(comments, :empty_list), :c), "
                             "last_comment_time = :t",
            ExpressionAttributeValues={
                ':c': [data],
                ':empty_list': [],
                ':t': create_time
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
