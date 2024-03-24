from flask import Flask, request, make_response
import boto3
import datetime
import json
import uuid
import decimal


application = Flask(__name__)

dynamo = boto3.resource('dynamodb', region_name='us-east-1')
datawarehouse = dynamo.Table('sku-warehouse-db')


def respond(err, res=None):


    if err:
        payload = {
            'status': 'failure',
            'message': str(err)
        }
    else:
        payload = {
            'status': 'success',
            'data': res
        }
        
    response = make_response(payload, 400 if err else 200)
    response.headers['Content-Type'] = 'application/json'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def err(msg):
    return respond(msg)

def ok(res):
    return respond(None, res)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
        
        
def validate_supported_keys(payload):
    supported_keys = {'quantity', 'name', 'description'}
    extra_keys = set(filter(lambda x: x not in supported_keys, payload.keys()))
    if extra_keys:
        raise ValueError('Unsupported Keys Found: ' + ', '.join(extra_keys) + ' Supported Keys: ' + ', '.join(supported_keys))
    else:
        print('Keys are valid')
        

@application.route("/ping")
def hello_world():
    return ok({"ping": "pong"})
    

@application.route("/item/<id>", methods=['GET'])
def get_one(id):
    try:
        print('Get Single Item: ' + str(id))
        return ok(datawarehouse.get_item(Key={'id': id}).get('Item'))
    except Exception as e:
        return err(str(e))

@application.route("/item/<id>", methods=['DELETE'])
def delete_one(id):
    try:
        print('Deleting Item: ' + str(id))
        datawarehouse.delete_item(Key={'id': id})
        return ok({'status': 'ok'})
    except Exception as e:
        return err(str(e))

@application.route("/item/<id>", methods=['PUT'])
def put_one(id):
    try:
        print('Updating Single Item: ' + id)
        payload = request.json
        validate_supported_keys(payload)
        payload['lastModified'] = str(datetime.datetime.utcnow())
        print(json.dumps(payload, indent=4))
    
        payload['id'] = id
        datawarehouse.put_item(Item=payload)
        return get_one(id)
    except Exception as e:
        return err(str(e))


@application.route("/items", methods=['GET'])
def get_all():
    try:
        res = datawarehouse.scan(TableName='sku-warehouse-db')
        print(res)
        return ok(res.get('Items', []))
    except Exception as e:
        return err(str(e))

@application.route("/items", methods=['POST'])
def insert_one():
    try:
        print("Insert New Item")
        payload = request.json
        print(json.dumps(payload, indent=4))
        validate_supported_keys(payload)
        payload['id'] = str(uuid.uuid4())
        payload['lastModified'] = str(datetime.datetime.utcnow())
        datawarehouse.put_item(Item=payload)
        return get_one(payload['id'])
    except Exception as e:
        return err(str(e))


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()
