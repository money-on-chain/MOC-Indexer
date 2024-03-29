from flask import Blueprint, request, current_app, Response
import database
from lib_tools import checkAddress, dump_dict_bson, mongodate_to_str, \
    getPagination

transactions = Blueprint('transactions', __name__, url_prefix="/api/v1/webapp/transactions")


@transactions.route("/list")
@transactions.route("/list/")
def tx_list():
    """
    URL: .../api/v1/webapp/transactions/list/
    URI FORMAT: ?address:<address>[&][token=<TOKEN>][&][limit=<RECORDS_LIMIT>][&][skip=<RECORDS_SKIPPED>]
    Fields:
        address (mandatory): Case insensitivity to match upper and lower cases
        token (optional): None for ALL, else, filtered as provided
        limit (optional, pagination): As it is, 20 or 40, default 20
        skip (optional, pagination): For skipping already queried records, default 0
    Complete INPUT and OUTPUT documentation: see Swagger file
    """
    if not database.checkConnection(current_app):
        return "Server error: unavailable connection to database", 500

    address = request.args.get('address', None)
    token = request.args.get('token', None)
    if not checkAddress(address):
        return "Invalid Address", 400

    pagination = getPagination(request)
    tx_col = database.db.get_collection(current_app.config.get("REQ_COLLECTIONS")['tx_col_name'])
    EXCLUDED_EVENTS = current_app.config.get('EXCLUDED_EVENTS')

    filter = {"address": {"$regex": address, '$options': 'i'},
              "event": {"$not": {"$in": EXCLUDED_EVENTS}}}

    if token is not None:
        filter["tokenInvolved"] = token

    lextract = tx_col.find(filter, {
        "_id": 1,
        "transactionHash": 1,
        "address": 1,
        "status": 1,
        "event": 1,
        "tokenInvolved": 1,
        "userAmount": 1,
        "lastUpdatedAt": 1,
        "createdAt": 1,
        "confirmationTime": 1,
        "confirmingPercent": 1,
        "RBTCAmount": 1,
        "RBTCTotal": 1,
        "USDAmount": 1,
        "USDCommission": 1,
        "USDInterests": 1,
        "USDTotal": 1,
        "amount": 1,
        "blockNumber": 1,
        "gasFeeRBTC": 1,
        "gasFeeUSD": 1,
        "isPositive": 1,
        "mocCommissionValue": 1,
        "mocPrice": 1,
        "processLogs": 1,
        "rbtcCommission": 1,
        "rbtcInterests": 1,
        "reservePrice": 1
    }).sort("createdAt", -1).skip(pagination.skip).limit(pagination.limit)
    records = list(lextract)
    records_count = len(records)

    for rec in records:
        rec['_id'] = str(rec['_id'])
        if rec['createdAt']:
            rec['createdAt'] = mongodate_to_str(rec['createdAt'])
        else:
            rec['createdAt'] = ''
        if rec['lastUpdatedAt']:
            rec['lastUpdatedAt'] = mongodate_to_str(rec['lastUpdatedAt'])
        else:
            rec['lastUpdatedAt'] = ''
        if rec['confirmationTime']:
            rec['confirmationTime'] = mongodate_to_str(rec['confirmationTime'])
        else:
            rec['confirmationTime'] = ''

    dict_values = {
        "transactions": records,
        "count": records_count,
        "total": tx_col.count_documents(filter)
    }
    return Response(dump_dict_bson(dict_values), mimetype="application/json")


@transactions.route("/last")
@transactions.route("/last/")
def tx_last():
    """
    URL: .../api/v1/woebapp/transactions/last/
    URI FORMAT: ?address:<address>[&][token=<TOKEN>]
    Fields:
        address (mandatory): Case insensitivity to match upper and lower cases
        token (optional): None for ALL, else, filtered as provided
    Complete INPUT and OUTPUT documentation: see Swagger file
    """
    if not database.checkConnection(current_app):
        return "Server error: unavailable connection to database", 500

    address = request.args.get('address', None)
    token = request.args.get('token', None)
    if not checkAddress(address):
        return "Invalid Address", 400

    tx_col = database.db.get_collection(current_app.config.get("REQ_COLLECTIONS")['tx_col_name'])
    EXCLUDED_EVENTS = current_app.config.get('EXCLUDED_EVENTS')

    filter = {"address": {"$regex": address, '$options': 'i'},
              "event": {"$not": {"$in": EXCLUDED_EVENTS}}}

    if token is not None:
        filter["tokenInvolved"] = token

    lextract = tx_col.find(filter, {
        "_id": 1,
        "transactionHash": 1,
        "address": 1,
        "status": 1,
        "event": 1,
        "tokenInvolved": 1,
        "lastUpdatedAt": 1,
        "createdAt": 1
    }).sort("lastUpdatedAt", -1).limit(1)

    last_operation = lextract[0]
    last_operation['_id'] = str(last_operation['_id'])
    if last_operation['createdAt']:
        last_operation['createdAt'] = mongodate_to_str(last_operation['createdAt'])
    else:
        last_operation['createdAt'] = ''
    if last_operation['lastUpdatedAt']:
        last_operation['lastUpdatedAt'] = mongodate_to_str(last_operation['lastUpdatedAt'])
    else:
        last_operation['lastUpdatedAt'] = ''

    return Response(dump_dict_bson(last_operation), mimetype="application/json")
