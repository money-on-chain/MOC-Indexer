FROM python:3.7

LABEL maintainer='martin.mulone@moneyonchain.com'

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /home/www-data && mkdir /home/www-data/app

WORKDIR /home/www-data/app/

COPY moc_indexer.py ./

COPY config.json ./

ENV PATH "$PATH:/home/www-data/app/"

ENV PYTHONPATH "${PYTONPATH}:/home/www-data/app/"

CMD [ "python", "./moc_indexer.py" ]
