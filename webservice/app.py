"""
Main program for the API Webservice IPFS WebApp

usage example:
    app.py --config ../settings/settings-moc-alpha-testnet-api.json --connection_network=None --config_network=None

mandatory params (config file or env):
    'port'; 'uri'; 'db'; 'pagination'; 'delta_price_hours'

optional params:
    'excluded_events'

blueprints:
    bp_main.py; bp_user_operations.py; bp_prices.py

diagnostic endpoints:
    ../infoapi ../ping

"""

import sys
import flask
import database
from bp_main import webapp_api
from bp_user_operations import transactions
from bp_prices import price_variation
from lib_tools import load_config
from webservice.bp_fastbtc_bridge import fastbtc

app = flask.Flask(__name__)

load_config(app)

database.dbConnect(app)

if not database.checkDBsOk(app):
    print("Required collections for the API not found in this database", file=sys.stderr)

app.register_blueprint(webapp_api)
app.register_blueprint(transactions)
app.register_blueprint(price_variation)
app.register_blueprint(fastbtc)


@app.errorhandler(500)
def internal_error(error):
    return "Server error", 500


if __name__ == "__main__":
    app.run(port=app.config.get('PORT'))
