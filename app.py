from flask import Flask, jsonify, request, redirect, url_for
import pymongo
from bson.objectid import ObjectId
from bson.json_util import dumps, loads
from werkzeug.exceptions import HTTPException
from functools import wraps

app = Flask(__name__)
client = pymongo.MongoClient()
apiConfig = client['api-config']
schemas = apiConfig['schemas']


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


def getDBName(username, databaseName):
    return f"{username}-{databaseName}"


def addToConfig(**kwargs):
    configDB = apiConfig['config']
    configDB.insert_one({**kwargs})


def dbExists(databaseName, storedDB):
    dblist = client.list_database_names()
    configDB = apiConfig['config']
    if storedDB not in dblist:
        if not configDB.find_one({"databaseName": databaseName}):
            return False
    return True


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


@app.errorhandler(mongoCustomException)
def handle_duplicate_resource(error):
    response = error.toJson()
    return response


def getUserName(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        print("wrapper running!")
        username = request.headers['username']
        return function(username, *args, **kwargs)
    return wrapper


def getActualDB(function):
    @wraps(function)
    def fnwrapper(username, databaseName, *args, **kwargs):
        print("wrapper running!")
        storedDB = getDBName(username, databaseName)
        return function(databaseName, storedDB, *args, **kwargs)
    return fnwrapper


def collectionExists(db, collectionName):
    if collectionName in db.list_collection_names():
        return True
    return False


@app.route('/')
def hello():
    return 'welcome to UDAPI'


@app.route('/databases/mongo/', methods=['POST'])
@getUserName
def createDatabase(username):
    configData = request.get_json()
    databaseName = configData['databaseName']
    processedDBName = getDBName(username, databaseName)
    if dbExists(databaseName, processedDBName):
        raise duplicateResource(
            f"The database '{databaseName}' already exists.")
    print('creating Db', processedDBName)
    client[processedDBName]
    addToConfig(**configData)
    return jsonify({'code': 200, 'message': f"Database '{databaseName}' created successfully", "success": 1})


@app.route('/databases/mongo/<databaseName>', methods=['DELETE'])
@getUserName
@getActualDB
def deleteDB(databaseName, storedDB):
    if not dbExists(databaseName, storedDB):
        raise notFound(f"Unknown database {databaseName}.")
    client.drop_database(storedDB)
    return jsonify({'code': 200, 'message': f"Database {databaseName} deleted successfully", "success": 1})


@app.route('/databases/mongo/<databaseName>/', methods=['GET'])
@getUserName
@getActualDB
def viewEntitySets(databaseName, storedDB):
    if not dbExists(databaseName, storedDB):
        raise notFound(f"Unknown database {databaseName}.")
    db = client[storedDB]
    print(dumps(db.list_collection_names()))
    return dumps({"message": db.list_collection_names(), "success": 1})


@app.route('/databases/mongo/<databaseName>/', methods=['POST'])
@getUserName
@getActualDB
def createEntitySet(databaseName, storedDB):
    db = client[storedDB]
    requestData = request.get_json()
    entitySet = requestData['entitySetName']
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySet):
            raise duplicateResource(
                f"Entity Set '{entitySet}' already exists.")
        else:

            newES = db[entitySet]
            collectionSchema = {'databaseName': storedDB}
            try:
                for entry in requestData["attributes"]:
                    collectionSchema[entry] = requestData['attributes'][entry]
                print(collectionSchema)
                schemas.insert_one(collectionSchema)
                newES.insert_one({'test': 'data'})
                newES.delete_one({'test': 'data'})
                return jsonify({'code': 200, 'message': f"Entity Set '{entitySet}' created successfully", "success": 1})
            except:
                return jsonify({'code': 400, 'message': f"Attributes have to be incuded in the request", "success": 0})
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/', methods=['PUT'])
@getUserName
@getActualDB
def updateEntitySetName(databaseName, storedDB, entitySetName):
    db = client[storedDB]
    eSet = db[entitySetName]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            newDbName = request.json['newEsName']
            eSet.rename(newDbName)
            return jsonify({'code': 200, 'message': f"Entity Set '{entitySetName}' updated to '{newDbName}' successfully", "success": 1})
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/', methods=['DELETE'])
@getUserName
@getActualDB
def deleteEntitySet(databaseName, storedDB, entitySetName):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            db.drop_collection(entitySetName)
            return jsonify({'code': 200, 'message': f"Entity Set '{entitySetName}' deleted successfully", "success": 1})
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/', methods=['GET'])
@getUserName
@getActualDB
def viewAllEntities(databaseName, storedDB, entitySetName):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            eSet = db[entitySetName]
            entities = eSet.find()
            results = []
            for entity in entities:
                results.append(entity)
            return dumps({"message": results, "success": 1})
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/', methods=['POST'])
@getUserName
@getActualDB
def createEntity(databaseName, storedDB, entitySetName):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            eSet = db[entitySetName]
            data = request.get_json()
            eSet.insert_one(data)
            return jsonify({'code': 200, 'message': f"Entity created successfully", "success": 1})
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/<primaryKey>/', methods=['PUT'])
@getUserName
@getActualDB
def updateEntityRecord(databaseName, storedDB, entitySetName, primaryKey):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            eSet = db[entitySetName]
            data = request.get_json()
            if eSet.find_one({"_id": ObjectId(primaryKey)}):
                eSet.find_one_and_update(
                    {"_id": ObjectId(primaryKey)}, {"$set": data})
                return jsonify({'code': 200, 'message': f"Entity updated successfully", "success": 1})
            else:
                raise notFound('Entity does not exist.')
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/<primaryKey>/', methods=['DELETE'])
@getUserName
@getActualDB
def deleteEntityRecord(databaseName, storedDB, entitySetName, primaryKey):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            eSet = db[entitySetName]
            if eSet.find_one({"_id": ObjectId(primaryKey)}):
                eSet.find_one_and_delete(
                    {"_id": ObjectId(primaryKey)})
                return jsonify({'code': 200, 'message': f"Entity deleted successfully", "success": 1})
            else:
                raise notFound('Entity does not exist.')
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


@app.route('/databases/mongo/<databaseName>/<entitySetName>/<primaryKey>/', methods=['GET'])
@getUserName
@getActualDB
def viewEntityRecord(databaseName, storedDB, entitySetName, primaryKey):
    db = client[storedDB]
    if dbExists(databaseName, storedDB):
        if collectionExists(db, entitySetName):
            eSet = db[entitySetName]
            result = eSet.find(
                {"_id": ObjectId(primaryKey)})
            if result != None:
                return dumps({"message": result, "success": 1})
            else:
                raise notFound('Entity does not exist.')
        else:
            raise notFound(f"Unknown entity set '{entitySetName}''")
    else:
        raise notFound(f"Unknown database {databaseName}.")


if __name__ == "__main__":
    app.run()
