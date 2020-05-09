from flask import Flask, jsonify, request, redirect, url_for
import pymongo
from bson.objectid import ObjectId
from bson.json_util import dumps
from werkzeug.exceptions import HTTPException


app = Flask(__name__)
client = pymongo.MongoClient()
apiConfig = client['api-config']


class mongoCustomException(Exception):
    def __init__(self):
        Exception.__init__(self)


class duplicateResource(mongoCustomException):
    def __init__(self, message):
        mongoCustomException.__init__(self)
        self.message = message
        self.code = 409
        self.success = 0

    def toJson(self):
        return dumps(self, default=lambda o: o.__dict__)


class notFound(mongoCustomException):
    def __init__(self, message):
        mongoCustomException.__init__(self)
        self.message = message
        self.code = 404
        self.success = 0

    def toJson(self):
        return dumps(self, default=lambda o: o.__dict__)


@app.errorhandler(mongoCustomException)
def handle_duplicate_resource(error):
    response = error.toJson()
    return response


def addToConfig(emailID, configData):
    configDB = apiConfig['config']
    configData['emailID'] = emailID
    configDB.insert_one(configData)
    print('stored in config')


@app.errorhandler(HTTPException)
def handle_exception(e):
    response = e.get_response()
    response.data = dumps({
        "code": e.code,
        "description": e.description,
        "success": 0
    })
    response.content_type = "application/json"
    return response


@app.route('/')
def hello():
    return 'welcome to UDAPI'


@app.route('/users', methods=['GET'])
def getUsers():
    usersDB = apiConfig['users']
    users = []
    for user in usersDB.find():
        users.append(user)
    return dumps(users)


@app.route('/users', methods=['POST'])
def addUser():
    usersDB = apiConfig['users']
    userData = request.get_json()
    if usersDB.find_one({"emailID": userData['emailID']}):
        raise duplicateResource(
            f"The user, {userData['emailID']}, already exists in the system.")
    usersDB.insert_one(userData)
    return jsonify({'message': 'User created successfully!'}), 200


@app.route('/users/<emailID>/databases/', methods=['GET'])
def viewDatabases(emailID):
    configDB = apiConfig['config']
    results = []
    for result in configDB.find({'emailID': emailID}):
        results.append(result['db_name'])
    return dumps(results)


@app.route('/users/<emailID>/databases/', methods=['POST'])
def createDatabase(emailID):
    configData = request.get_json()
    print(configData)
    db_name = configData['db_name']
    client[db_name]
    print('db created')
    addToConfig(emailID, configData)
    return 'done'


@app.route('/users/<emailID>/databases/<databaseName>', methods=['GET'])
def getConfigDetailsForOneDB(emailID, databaseName):
    configDB = apiConfig['config']
    result = configDB.find_one({"emailID": emailID, "db_name": databaseName})
    return dumps(result)


@app.route('/users/<emailID>/databases/<databaseName>', methods=['PUT'])
def updateDBConfig(emailID, databaseName):
    data = request.get_json()
    configDB = apiConfig['config']
    configDB.find_one_and_update(
        {"emailID": emailID, "db_name": databaseName}, {"$set": data})
    return 'edited'


@app.route('/users/<emailID>/databases/<databaseName>', methods=['DELETE'])
def deleteDB(emailID, databaseName):
    configDB = apiConfig['config']
    configDB.find_one_and_delete(
        {"emailID": emailID, "db_name": databaseName})
    client.drop_database(databaseName)
    return 'deleted'


@app.route('/users/<emailID>/databases/<databaseName>/', methods=['GET'])
def viewEntitySets(emailID, databaseName):
    db = client[databaseName]
    return dumps(db.list_collection_names())


@app.route('/users/<emailID>/databases/<databaseName>/', methods=['POST'])
def createEntitySet(emailID, databaseName):
    db = client[databaseName]
    entitySet = request.json['entity_set_name']
    newES = db[entitySet]
    newES.insert_one({'test': 'data'})
    newES.delete_one({'test': 'data'})
    return 'created'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/', methods=['PUT'])
def updateEntitySetName(emailID, databaseName, entitySet):
    db = client[databaseName]
    eSet = db[entitySet]
    newDbName = request.json['newDbName']
    eSet.rename(newDbName)
    return 'updated'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/', methods=['DELETE'])
def deleteEntitySet(emailID, databaseName, entitySet):
    db = client[databaseName]
    db.drop_collection(entitySet)
    return 'deleted'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/', methods=['GET'])
def viewAllEntities(emailID, databaseName, entitySet):
    db = client[databaseName]
    eSet = db[entitySet]
    entities = eSet.find()
    results = []
    for entity in entities:
        results.append(entity)
    return dumps(results)


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/', methods=['POST'])
def createEntity(emailID, databaseName, entitySet):
    db = client[databaseName]
    eSet = db[entitySet]
    data = request.get_json()
    entities = eSet.insert_one(data)
    return 'done'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/<primaryKey>/', methods=['PUT'])
def updateEntityRecord(emailID, databaseName, entitySet, primaryKey):
    db = client[databaseName]
    eSet = db[entitySet]
    data = request.get_json()
    eSet.find_one_and_update(
        {"_id": ObjectId(primaryKey)}, {"$set": data})
    return 'edited'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/<primaryKey>/', methods=['DELETE'])
def deleteEntityRecord(emailID, databaseName, entitySet, primaryKey):
    db = client[databaseName]
    eSet = db[entitySet]
    eSet.find_one_and_delete(
        {"_id": ObjectId(primaryKey)})
    return 'deleted'


@app.route('/users/<emailID>/databases/<databaseName>/<entitySet>/<primaryKey>/', methods=['GET'])
def viewEntityRecord(emailID, databaseName, entitySet, primaryKey):
    db = client[databaseName]
    eSet = db[entitySet]
    result = eSet.find(
        {"_id": ObjectId(primaryKey)})
    return dumps(result)


if __name__ == "__main__":
    app.run()
