# MoC Indexer

## Introduction

To speed up the app we need an indexer of the blockchain of our contracts. 
The indexer query the status of the contracts
and write to mongo database, so the app query the mongo instead of blockchain (slow).


### Indexer jobs

 1. **Scan Blocks**: Indexing events transactions
 2. **Scan Prices**: Scan prices 
 3. **Scan Moc State**: Scan current moc state
 4. **Scan Moc Status**
 5. **Scan MocState Status**
 6. **Scan User State Update** 
 7. **Scan Blocks not processed**
 99. **Reconnect on lost chain**
 

### Usage

**Requirement and installation**
 
* We need Python 3.6+
* Brownie

Install libraries

`pip install -r requirements.txt`

[Brownie](https://github.com/eth-brownie/brownie) is a Python-based development and testing framework for smart contracts.
Brownie is easy so we integrated it with Money on Chain.

`pip install eth-brownie==1.14.6`

**Network Connections**

First we need to install custom networks (RSK Nodes) in brownie:

```
console> brownie networks add RskNetwork rskTestnetPublic host=https://public-node.testnet.rsk.co chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskTestnetLocal host=http://localhost:4444 chainid=31 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskMainnetPublic host=https://public-node.rsk.co chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
console> brownie networks add RskNetwork rskMainnetLocal host=http://localhost:4444 chainid=30 explorer=https://blockscout.com/rsk/mainnet/api
brownie networks add BSCNetwork bscTestnet host=https://data-seed-prebsc-1-s1.binance.org:8545/ chainid=97 explorer=https://blockscout.com/rsk/mainnet/api
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

`python ./app_run_moc_indexer.py --config=settings/aws-moc-alpha-testnet.json --config_network=mocTestnetAlpha --connection_network=rskTestnetPublic`

**--config:** Path to config.json 

**--config_network=mocTestnetAlpha:** Config Network name in the json

**--connection_network=rskTestnetPublic:** Connection network in brownie 


**Usage Docker**

Build

```
bash ./docker_build.sh -e ec2_alphatestnet -c ./settings/aws-moc-alpha-testnet.json
```

Run

```
docker run -d \
--name ec2_alphatestnet_1 \
--env APP_MONGO_URI=mongodb://192.168.56.2:27017/ \
--env APP_MONGO_DB=local_alpha_testnet2 \
--env APP_CONFIG_NETWORK=mocTestnetAlpha \
--env APP_CONNECTION_NETWORK=https://public-node.testnet.rsk.co,31 \
moc_indexer_ec2_alphatestnet
```
  
### Custom node

**APP_CONNECTION_NETWORK:** https://public-node.testnet.rsk.co,31



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


### AWS **Building image** 

```
./aws_build_and_push.sh -e <environment> -c <config file> -i <aws id>
```

 Where environment could be

* ec2_alphatestnet: alpha-testnet.moneyonchain.com
* ec2_testnet: moc-testnet.moneyonchain.com
* ec2_mainnet: alpha.moneyonchain.com
* ec2_rdoc_mainnet: rif.moneyonchain.com
* ec2_rdoc_testnet: rif-testnet.moneyonchain.com
* ec2_rdoc_alphatestnet: rif-alpha.moneyonchain.com

Finally it will build the docker image.


**Example:**

Before pushing the image, we need to check if ecr image exist, go to [https://us-west-1.console.aws.amazon.com/ecr/repositories?region=us-west-1](https://us-west-1.console.aws.amazon.com/ecr/repositories?region=us-west-1) and create it

Ensure you have installed the latest version of the AWS CLI and Docker.

Make sure you have built your image before pushing it. 

This script will tag with _latest_ and push to the proper repository.

```
$ ./aws_build_and_push.sh -e ec2_alphatestnet -c ./settings/aws-moc-mainnet2.json -i 123456 
```

## Setting up in AWS ECS

On the task definition it's important to set up the proper environment variables.

1. APP_CONFIG: The config.json you find in your _settings/deploy_XXX.json folder as json
2. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY: these are needed for the heartbeat function of the jobs, as it needs an account that has write access to a metric in Cloudwatch
3. APP_CONFIG_NETWORK: The network here is listed in APP_NETWORK
4. APP_CONNECTION_NETWORK: The network here is listed in APP_CONNECTION_NETWORK
5. APP_MONGO_URI: mongo uri
6. APP_MONGO_DB: mongo db

