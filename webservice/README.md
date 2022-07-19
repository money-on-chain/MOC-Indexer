# MoC - API Webservice IPFS WebApp (WIP)
*API for requests of user operations & prices of Moc-Indexer

## General specifications

The API solution uses Flask web microframework with Blueprint module and PyMongo distribution tools for connecting to MongoDB (see "requirements.txt").

### Python version
Python 3.7.5+ support

### MongoDB version
MongoDB	4.4.9+ support

### API listening port:
Customizable (see configuration file: 'port')

### Environment Variables
Database server connection
- "APP_MONGO_URI"
- "APP_MONGO_DB"
- see other mandory fields for the functional services: 'pagination'; 'delta_price_hours'

### Python dependencies
- `pip install -r requirements.txt`

### Usage

Run:

`python app.py --config ./settings/develop.json`

### API specification
- TBD Swagger docs.

### Technical Specs (WIP)

    MAIN MODULE
        app

        DETAIL
            Main program for the API Webservice IPFS WebApp

            usage example:
                app.py --config ./settings/settings-moc-alpha-testnet.json

            mandatory params (config file or env):
                'port'; 'uri'; 'db'; 'pagination'; 'delta_price_hours'

            optional params:
                'excluded_events'

            blueprints:
                bp_main.py; bp_user_operations.py; bp_prices.py

            diagnostic endpoints:
                ../infoapi ../ping


    BLUEPRINTS & ENDPOINTs SPECs

        NAME
            bp_user_operations

            FUNCTIONS
                tx_last()
                    URL: .../api/v1/woebapp/transactions/last/
                    URI FORMAT: ?address:<address>[&][token=<TOKEN>]
                    Fields:
                        address (mandatory): Case insensitivity to match upper and lower cases
                        token (optional): None for ALL, else, filtered as provided
                    Complete INPUT and OUTPUT documentation: see Swagger file
                
                tx_list()
                    URL: .../api/v1/webapp/transactions/list/
                    URI FORMAT: ?address:<address>[&][token=<TOKEN>][&][limit=<RECORDS_LIMIT>][&][skip=<RECORDS_SKIPPED>]
                    Fields:
                        address (mandatory): Case insensitivity to match upper and lower cases
                        token (optional): None for ALL, else, filtered as provided
                        limit (optional, pagination): As it is, 20 or 40, default 20
                        skip (optional, pagination): For skipping already queried records, default 0
                    Complete INPUT and OUTPUT documentation: see Swagger file

            DATA
                transactions = <Blueprint 'transactions'>

        NAME
            bp_prices

            FUNCTIONS
                variation()
                    URL: .../api/v1/woebapp/prices/var/
                    Complete INPUT and OUTPUT documentation: see Swagger file

            DATA
                price_variation = <Blueprint 'price_variation'>

