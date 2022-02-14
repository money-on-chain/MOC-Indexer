
class BaseIndexEvent:

    name = 'Name'
    precision = 10 ** 18

    def __init__(self, options, app_mode):

        self.options = options
        self.app_mode = app_mode

    def status_tx(self, parse_receipt):

        if parse_receipt["blockNumber"] - parse_receipt['chain']['last_block'] > parse_receipt['chain']['confirm_blocks']:
            status = 'confirmed'
            confirmation_time = parse_receipt['chain']['block_ts']
        else:
            status = 'confirming'
            confirmation_time = None

        return status, confirmation_time
