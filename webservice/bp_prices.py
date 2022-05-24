from flask import Blueprint, current_app, Response
from lib_tools import dump_dict_bson, mongodate_to_str
import database
from datetime import timedelta

price_variation = Blueprint('price_variation', __name__, url_prefix="/api/v1/webapp/prices")


@price_variation.route('/var')
@price_variation.route('/var/')
def variation():
    """
    URL: .../api/v1/woebapp/prices/var/
    Complete INPUT and OUTPUT documentation: see Swagger file
    """
    if not database.checkConnection(current_app):
        return "Server error: unavailable connection to database", 500

    price_col = database.db.get_collection(current_app.config.get("REQ_COLLECTIONS")['price_col_name'])
    DELTA_HOURS = current_app.config.get('DELTA_PRICE_HOURS')

    lextract = price_col.find({}, {
        "_id": 1,
        "blockHeight": 1,
        "bitcoinPrice": 1,
        "bproDiscountPrice": 1,
        "bproPriceInRbtc": 1,
        "bproPriceInUsd": 1,
        "bprox2PriceInBpro": 1,
        "bprox2PriceInRbtc": 1,
        "bprox2PriceInUsd": 1,
        "createdAt": 1,
        "reservePrecision": 1
    }).sort("blockHeight", -1).limit(1)
    current_prices = lextract[0]

    current_prices['_id'] = str(current_prices['_id'])
    current_date = current_prices['createdAt']
    current_prices['createdAt'] = mongodate_to_str(current_prices['createdAt'])

    delta_date = current_date - timedelta(hours=DELTA_HOURS)
    delta_date_floor = delta_date.replace(hour=0, minute=0)

    filter = {"createdAt": {"$gte": delta_date_floor, "$lt": delta_date}}
    lextract2 = price_col.find(filter, {
        "_id": 1,
        "blockHeight": 1,
        "bitcoinPrice": 1,
        "bproDiscountPrice": 1,
        "bproPriceInRbtc": 1,
        "bproPriceInUsd": 1,
        "bprox2PriceInBpro": 1,
        "bprox2PriceInRbtc": 1,
        "bprox2PriceInUsd": 1,
        "createdAt": 1,
        "reservePrecision": 1
    }).sort("blockHeight", -1).limit(1)
    delta_prices = lextract2[0]

    delta_prices['_id'] = str(delta_prices['_id'])
    delta_prices['createdAt'] = mongodate_to_str(delta_prices['createdAt'])

    dict_values = {
        "current": current_prices,
        str(DELTA_HOURS)+"hs": delta_prices
    }
    return Response(dump_dict_bson(dict_values), mimetype="application/json")
