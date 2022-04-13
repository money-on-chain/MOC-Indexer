import flask
import database

webapp_api = flask.Blueprint('webapp_api', __name__)

@webapp_api.route("/infoapi/")
@webapp_api.route("/infoapi")
def infoabi():
    dInfoApi = {}
    dInfoApi["webAppAPI"] = flask.current_app.config.get('API_VERSION')
    dInfoApi["Flask"] = flask.__version__
    dInfoApi["DataBaseData"] = database.get_db_info(flask.current_app)
    return dInfoApi, 200

@webapp_api.route("/ping/")
@webapp_api.route("/ping")
def ping():
    return "webAppAPI OK", 200


@webapp_api.route("/")
def root_route():
    return "", 200
