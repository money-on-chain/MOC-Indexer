import string
import sys
import os
from bson.json_util import dumps

app_module_file = __file__
app_module_folder = os.path.abspath(os.path.join(app_module_file, os.pardir))
app_module_parent = os.path.abspath(os.path.join(app_module_folder, os.pardir))
sys.path.append(app_module_parent)
try:
    from config_parser import ConfigParser
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Custom MoC module config_parser not found in expected path - it is required for this API")

mongodate_to_utc = lambda x: int(x.timestamp())

mongodate_to_str = lambda x: str(x.isoformat(timespec='milliseconds'))+"Z"

def checkAddress(address):
    if address is None or len(address) < 25 or address[:2].lower() != "0x":
        return False
    return all(x in string.hexdigits for x in address[2:])

def dump_dict_bson(dict):
    return dumps(dict, indent=4)

def load_config(app):
    cfg = ConfigParser()
    app.config['API_VERSION'] = "202204201812"
    app.config['PORT'] = cfg.config['webapp_api_settings']['port']
    app.config['URI_PARAM'] = cfg.config['mongo']['uri']
    app.config['MONGO_DB'] = cfg.config['mongo']['db']
    app.config['PAGINATION'] = cfg.config['webapp_api_settings']['pagination']
    app.config['EXCLUDED_EVENTS'] = cfg.config['webapp_api_settings']['excluded_events']
    app.config['DELTA_PRICE_HOURS'] = cfg.config['webapp_api_settings']['delta_price_hours']
    app.config['DEBUG_MODE'] = cfg.config['debug']

    app.config['REQ_COLLECTIONS'] = {'tx_col_name': 'Transaction',
                                     'price_col_name': 'Price'}
