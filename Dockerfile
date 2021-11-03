FROM python:3.9

LABEL maintainer='martin.mulone@moneyonchain.com'

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# brownie install connections
RUN brownie networks add RskNetwork rskTestnetPublic host=https://public-node.testnet.rsk.co chainid=31 explorer=https://blockscout.com/rsk/mainnet/api timeout=180
RUN brownie networks add RskNetwork rskTestnetPrivate host=http://moc-rsk-node-testnet.moneyonchain.com:4454 chainid=31 explorer=https://blockscout.com/rsk/mainnet/api timeout=180
RUN brownie networks add RskNetwork rskMainnetPublic host=https://public-node.rsk.co chainid=30 explorer=https://blockscout.com/rsk/mainnet/api timeout=180
RUN brownie networks add RskNetwork rskMainnetPrivate host=http://moc-rsk-node-mainnet.moneyonchain.com:4454 chainid=30 explorer=https://blockscout.com/rsk/mainnet/api timeout=180

RUN mkdir /home/www-data && mkdir /home/www-data/app

ARG CONFIG=config.json

WORKDIR /home/www-data/app/
COPY add_custom_network.sh ./
COPY app_run_moc_indexer.py ./
ADD $CONFIG ./config.json
COPY config_parser.py ./
COPY indexer/ ./indexer/

ENV PATH "$PATH:/home/www-data/app/"
ENV AWS_DEFAULT_REGION=us-west-1

ENV PYTHONPATH "${PYTONPATH}:/home/www-data/app/"

#CMD [ "python", "./app_run_moc_indexer.py" ]
CMD /bin/bash -c 'bash ./add_custom_network.sh; python ./app_run_moc_indexer.py'