import string
from bson.json_util import dumps
from config_parser import ConfigParser

mongodate_to_utc = lambda x: int(x.timestamp())

def checkAddress(address):
    if address is None or len(address) < 25 or address[:2].lower() != "0x":
        return False
    return all(x in string.hexdigits for x in address[2:])

def dump_dict_bson(dict):
    return dumps(dict, indent=4)

def load_config(app):
    cfg = ConfigParser()
    app.config['API_VERSION'] = "202204131631"
    app.config['PORT'] = cfg.config['webapp_api_settings']['port']
    app.config['URI_PARAM'] = cfg.config['mongo']['uri']
    app.config['MONGO_DB'] = cfg.config['mongo']['db']
    app.config['PAGINATION'] = cfg.config['webapp_api_settings']['pagination']
    app.config['EXCLUDED_EVENTS'] = cfg.config['webapp_api_settings']['excluded_events']
    app.config['DELTA_PRICE_HOURS'] = cfg.config['webapp_api_settings']['delta_price_hours']
    app.config['DEBUG_MODE'] = cfg.config['debug']

    app.config['REQ_COLLECTIONS'] = {'tx_col_name': 'Transaction',
                                     'price_col_name': 'Price'}
