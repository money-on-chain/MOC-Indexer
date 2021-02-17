# MoC Indexer

## Introduction

To speed up the app we need an indexer of the blockchain of our contracts. 
The indexer query the status of the contracts
and write to mongo database, so the app query the mongo instead of blockchain (slow).


### Usage

**Requirement and installation**
 
* We need Python 3.6+
* Brownie

Install libraries

`pip install -r requirements.txt`

[Brownie](https://github.com/eth-brownie/brownie) is a Python-based development and testing framework for smart contracts.
Brownie is easy so we integrated it with Money on Chain.

`pip install eth-brownie==1.12.2`

**Network Connections**

First we need to install custom networks (RSK Nodes) in brownie:

```
console> brownie networks add RskNetwork rskTestnetPublic host=https://public-node.testnet.rsk.co chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskTestnetLocal host=http://localhost:4444 chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskMainnetPublic host=https://public-node.rsk.co chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskMainnetLocal host=http://localhost:4444 chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
```

**Connection table**

| Network Name      | Network node          | Host                               | Chain    |
|-------------------|-----------------------|------------------------------------|----------|
| rskTestnetPublic   | RSK Testnet Public    | https://public-node.testnet.rsk.co | 31       |    
| rskTestnetLocal    | RSK Testnet Local     | http://localhost:4444              | 31       |
| rskMainnetPublic  | RSK Mainnet Public    | https://public-node.rsk.co         | 30       |
| rskMainnetLocal   | RSK Mainnet Local     | http://localhost:4444              | 30       |


**Usage**

**Example**

Make sure to change **settings/settings-xxx.json** to point to your mongo db.

`python ./app_run_moc_indexer.py --config=settings/settings-moc-testtyd-martin.json --config_network=mocTestTyD --connection_network=rskTestnetPublic`

**--config:** Path to config.json 

**--config_network=mocTestTyD:** Config Network name in the json

**--connection_network=rskTestnetPublic:** Connection network in brownie 


**Usage Docker**

Build

```
./build.sh -e ec2_tyd
```

Run

```
docker run -d \
--name ec2_tyd_1 \
--env APP_MONGO_URI=mongodb://192.168.56.2:27017/ \
--env APP_MONGO_DB=local_tyd \
--env APP_CONFIG_NETWORK=mocTestTyD \
--env APP_CONNECTION_NETWORK=rskTesnetPublic \
moc_indexer_ec2_tyd
```
  



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
3. APP_CONFIG_NETWORK: The network here is listed in APP_NETWORK
3. APP_CONNECTION_NETWORK: The network here is listed in APP_CONNECTION_NETWORK


