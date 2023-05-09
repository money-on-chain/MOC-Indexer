import pymongo
import logging
import sys

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)

serverDB = db = None

def checkDBsOk(app):
    return all(elem in db.list_collection_names() for elem in
               list(app.config.get('REQ_COLLECTIONS').values()))


def dbConnect(app):
    global serverDB, db
    DEBUG = app.config.get('DEBUG_MODE')
    uri_param = app.config.get('URI_PARAM')
    db_param = app.config.get('MONGO_DB')
    if uri_param is None or db_param is None:
        raise Exception("Parameters for database not found")
    serverDB = pymongo.MongoClient(uri_param, connectTimeoutMS=1000, SocketTimeoutMS=1000, serverSelectionTimeoutMS=1000)
    db = serverDB[db_param]
    try:
        colCtrl = db.command("dbStats")["collections"]
        log.info("Connection to MongoDB OK", exc_info=DEBUG)
    except Exception as e:
        raise Exception("Server error: unavailable connection to database")
    if colCtrl == 0:
        print("Invalid database or database with no collections", file=sys.stderr)
    log.info("Database connection and settings OK", exc_info=app.config.get('DEBUG_MODE'))


def checkConnection(app):
    try:
        if db.command("connectionStatus")['ok']:
            return True
        else:
            return False
    except Exception as e:
        log.error("Connection to database delayed or refused", exc_info=app.config.get('DEBUG_MODE'))
        return False


def get_db_info(app):
    dbInfo = {}
    if checkConnection(app):
        dbInfo["MongoDB"] = serverDB.server_info()['version']
    else:
        dbInfo["MongoDB"] = "Unavailable connection to database"
    dbInfo["PyMongo"] = pymongo.version
    dbInfo['DB_Collections_Ok'] = checkDBsOk(app)
    return dbInfo
