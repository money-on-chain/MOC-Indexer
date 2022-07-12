#!/bin/bash

# exit as soon as an error happen
set -e

# Create image that will compile the app into a bundle
docker build -t nginx_api_ipfs -f Docker/Dockerfile.nginx .
