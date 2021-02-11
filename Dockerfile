FROM python:3.7

LABEL maintainer='martin.mulone@moneyonchain.com'

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# brownie install connections
RUN brownie networks add RskNetwork rskTesnetPublic host=https://public-node.testnet.rsk.co chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
RUN brownie networks add RskNetwork rskTesnetPrivate host=http://moc-rsk-node-testnet.moneyonchain.com:4454 chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
RUN brownie networks add RskNetwork rskTesnetCustom host=$BROWNIE_CUSTOM_HOST_TESTNET chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
RUN brownie networks add RskNetwork rskMainnetPublic host=https://public-node.rsk.co chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
RUN brownie networks add RskNetwork rskMainnetPrivate host=http://moc-rsk-node-mainnet.moneyonchain.com:4454 chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
RUN brownie networks add RskNetwork rskMainnetCustom host=$BROWNIE_CUSTOM_HOST_MAINNET chainid=30 explorer=https://blockscout.com/rsk/mainnet/api

RUN mkdir /home/www-data && mkdir /home/www-data/app

WORKDIR /home/www-data/app/

COPY app_run_moc_indexer.py ./
COPY config.json ./
COPY config_parser.py ./
COPY indexer/ ./indexer/

ENV PATH "$PATH:/home/www-data/app/"
ENV AWS_DEFAULT_REGION=us-west-1

ENV PYTHONPATH "${PYTONPATH}:/home/www-data/app/"

CMD [ "python", "./app_run_moc_indexer.py" ]
