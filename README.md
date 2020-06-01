# MoC Indexer

## Introduction

Blockchain indexer moc contracts

### Usage

**Requirement and installation**
 
*  We need Python 3.6+

Install libraries

`pip install -r requirements.txt`

**Usage**

Make sure to change **config.json** to point to your network.

`python run_indexer.py`

Alternatives:

`python run_indexer.py --network=mocTestnetAlpha`

**--config:** Path to config.json 

**--network=mocTestnetAlpha:** Network name in the json


**Usage Docker**

Build

```
docker build -t moc_indexer -f Dockerfile .
```

Run

```
docker run -d \
--name moc_indexer_1 \
--env MOC_JOBS_NETWORK=mocTestnetAlpha \
moc_indexer
```
 