from flask import Blueprint, request, current_app, Response
import database
from lib_tools import checkAddress, dump_dict_bson, mongodate_to_str

fastbtc = Blueprint('fastbtc', __name__, url_prefix="/api/v1/webapp/fastbtc")


@fastbtc.route("/pegout/")
def pegout():
    """
    URL: .../api/v1/webapp/fastbtc/pegout
    URI FORMAT: ?address:<address>[&][limit=<RECORDS_LIMIT>][&][skip=<RECORDS_SKIPPED>]
    Fields:
        address (mandatory): Case insensitivity to match upper and lower cases
        limit (optional, pagination): As it is, 20 or 40, default 20
        skip (optional, pagination): For skipping already queried records, default 0
    Complete INPUT and OUTPUT documentation: see Swagger file
    """
    if not database.checkConnection(current_app):
        return "Server error: unavailable connection to database", 500

    address = request.args.get('address', None)
    if not checkAddress(address):
        return "Invalid Address", 400

    SKIP_RECORDS = int(request.args.get('skip', 0))
    LIMIT_PAGE = int(request.args.get('limit', 0))
    DEFAULT_PAGINATIONS = current_app.config.get('PAGINATION')

    if LIMIT_PAGE not in DEFAULT_PAGINATIONS:
        LIMIT_PAGE = DEFAULT_PAGINATIONS[0]

    # tx_col = database.db.get_collection(current_app.config.get("REQ_COLLECTIONS")['tx_col_name'])
    tx_col = database.db.get_collection('FastBtcBridge')
    filter = {"rskAddress": {"$regex": address, '$options': 'i'},
              "type": "PEG_OUT",
              }

    lextract = tx_col.find(filter, {
        "transactionHashLastUpdated": 0,
        "updated": 0
    }).sort("timestamp", -1).skip(SKIP_RECORDS).limit(LIMIT_PAGE)
    records = list(lextract)

    for rec in records:
        rec['_id'] = str(rec['_id'])
        rec['timestamp'] = mongodate_to_str(rec['timestamp'])
        # rec['updated'] = mongodate_to_str(rec['updated'])

    dict_values = {
        "transactions": records,
    }

    return Response(dump_dict_bson(dict_values), mimetype="application/json")
