FROM python:3.7

LABEL maintainer='martin.mulone@moneyonchain.com'

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /home/www-data && mkdir /home/www-data/app

WORKDIR /home/www-data/app/

COPY app_run_indexer.py ./

COPY moc_indexer.py ./
COPY taskrunner.py ./
COPY common.py ./
COPY moc_config.py ./
COPY jobs.py ./
COPY agent/ ./agent

ENV PATH "$PATH:/home/www-data/app/"
ENV AWS_DEFAULT_REGION=us-west-1
ARG configFile

ENV APP_CONFIG=${configFile}
ENV environment=${env}
COPY ./settings/${configFile} ./

ENV PYTHONPATH "${PYTONPATH}:/home/www-data/app/"

#CMD [ "python", "./app_run_indexer.py" ]
CMD [ "sh","-c","python ./taskrunner.py -n ${environment} -c ${configFile} jobs:* agent.jobs:*"]
#python taskrunner.py -n mocTestnetAlpha -c config-sample.json jobs:* agent.jobs:*
