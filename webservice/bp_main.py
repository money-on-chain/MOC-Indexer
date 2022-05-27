import flask
from flask import Response

import database
from lib_tools import dump_dict_bson

webapp_api = flask.Blueprint('webapp_api', __name__)


@webapp_api.route("/infoapi/")
@webapp_api.route("/infoapi")
def infoabi():
    dInfoApi = {"webAppAPI": flask.current_app.config.get('API_VERSION'),
                "Flask": flask.__version__,
                "DataBaseData": database.get_db_info(flask.current_app)}
    return Response(dump_dict_bson(dInfoApi), mimetype="application/json")


@webapp_api.route("/ping/")
@webapp_api.route("/ping")
def ping():
    return "webAppAPI OK", 200


@webapp_api.route("/")
def root_route():
    return "", 200
