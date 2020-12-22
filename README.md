# MoC Indexer

## Introduction

to speed up the app we need an indexer of the blockchain of our contracts. The indexer query the status of the contracts
and write to mongo database, so the app query the mongo instead of blockchain (slow).

This tasks run every time, updating the mongo database with the blockchain information. We have 5 tasks running independently:

1) Scan MoC Blocks. Run every 20 seconds. This tasks watch get the new blocks and filter if there are any transactions 
corresponding to our contracts or tokens. 

2) Scan MoC Prices. Run every 20 seconds. Query the prices for the new blocks.

3) Scan MoC States. Run every 20 seconds. Query the MoCState contract, here we have a lot of status of MOC like prices, 
cobj, leverage, etc. Also save historic states.

4) Scan MoC Status. Run every 10 seconds. Status of the transactions

5) Scan MoC State Status. Keep information about if there are price actives.


### Usage

**Requirement and installation**
 
*  We need Python 3.6+

Install libraries

`pip install -r requirements.txt`

**Usage**

Make sure to change **settings/settings-xxx.json** to point to your mongo db.

`python ./app_run_indexer.py --config=settings/settings-xxx.json --network=develop`

**--config:** Path to config.json 

**--network=mocTestnetAlpha:** Network name in the json


**Usage**

Alternative usages:

* Single task scripts:

`python ./app_scan_moc_blocks.py --config=settings/settings-xxx.json 
--network=develop`

`python ./app_scan_moc_prices.py --config=settings/settings-xxx.json 
--network=develop`

`python ./app_scan_moc_state.py --config=settings/settings-xxx.json 
--network=develop`

`python ./app_scan_moc_status.py --config=settings/settings-xxx.json 
--network=develop`

* Full tasks scripts:

`python ./app_run_indexer.py --config=settings/settings-xxx.json 
--network=develop` _(which choose to run jobs or history jobs based on config)_

* Via `taskrunner`:

`python taskrunner.py -c config -n network jobs:*` _for default jobs_
`python taskrunner.py -c config -n network jobs_history:*` _for history jobs_
`python taskrunner.py -c config -n network jobs:scan_moc_blocks` _for 
scan_moc_blocks job_
`python taskrunner.py -c config -n network jobs:scan_moc_prices` _for scan_moc_prices job_
`python taskrunner.py -c config -n network jobs:scan_moc_status` _for scan_moc_status job_
`python taskrunner.py -c config -n network jobs:scan_moc_state` _for scan_moc_state job_



**Example**

Make sure to change **settings/settings-moc-alpha-testnet.json** to point to your **mongo db**.

`python ./app_run_indexer.py --config=settings/settings-moc-alpha-testnet.json --network=mocTestnetAlpha`



## AWS


### **Starting building server**

First you have to start the building server in EC2

Connect to builder with bastion

```
ssh -F /home/martin/.ssh/bastion/moc_ssh_config moc-builder
```

change user to builder

```
sudo su builder -s /bin/bash
```


### **Building image** 

```
./build.sh -e <environment>
```

 Where environment could be

* ec2_alphatestnet: alpha-testnet.moneyonchain.com
* ec2_testnet: moc-testnet.moneyonchain.com
* ec2_mainnet: alpha.moneyonchain.com
* ec2_rdoc_mainnet: rif.moneyonchain.com
* ec2_rdoc_testnet: rif-testnet.moneyonchain.com
* ec2_rdoc_alphatestnet: rif-alpha.moneyonchain.com

Finally it will build the docker image.


### Pushing Image to repository

Before pushing the image, we need to check if ecr image exist, go to [https://us-west-1.console.aws.amazon.com/ecr/repositories?region=us-west-1](https://us-west-1.console.aws.amazon.com/ecr/repositories?region=us-west-1) and create it

Ensure you have installed the latest version of the AWS CLI and Docker.

Make sure you have built your image before pushing it. Then execute **./tag_and_push.sh -e  &lt;environment>**

This script will tag with _latest_ and push to the proper repository.

```
$ ./tag_and_push.sh -e ec2_alphatestnet
```

Result 

```

WARNING! Using --password via the CLI is insecure. Use --password-stdin.
Login Succeeded
The push refers to repository [551471957915.dkr.ecr.us-west-1.amazonaws.com/moc_jobs_moc-alphatestnet]
1ec27b2766b2: Pushed 
45dbe5a18fd6: Pushed 
4d4bec2e685f: Pushed 
9da1af2983d7: Pushed 
b24985db57b9: Pushed 
d22eb95b7a94: Pushed 
bb1af9323ea6: Pushed 
48ea5aa9c3a2: Pushed 
c2dfe15f7892: Pushed 
013b3c7b17e1: Pushed 
8c40e5337dcd: Pushed 
978eb45ee4b6: Pushed 
3f53405f239c: Pushed 
48ebd1638acd: Pushed 
31f78d833a92: Pushed 
2ea751c0f96c: Pushed 
7a435d49206f: Pushed 
9674e3075904: Pushed 
831b66a484dc: Pushed 
latest: digest: sha256:131df4bd072586f2808143129d8d396dcf304f758771c46b3470ae474fbf0e37 size: 4306
```

Image should be now available in the AWS repository for Fargate usage

## Setting up in AWS ECS

On the task definition it's important to set up the proper environment variables.

1. APP_CONFIG: The config.json you find in your _settings/deploy_XXX.json folder as json
2. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY: these are needed for the heartbeat function of the jobs, as it needs an account that has write access to a metric in Cloudwatch
3. APP_NETWORK: The network here is listed in APP_NETWORK


