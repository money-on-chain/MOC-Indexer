#!/bin/bash

set -e

usage() { echo "Usage: $0 -e <environment> " 1>&2; exit 1; }

while getopts ":e:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "alpha-testnet" || e == "testnet" || e== "mainnet" )) || usage
            case $e in
                alpha-testnet)
                    ENV=$e
                    ;;
                testnet)      
                    ENV=$e
                    ;;
                mainnet)
                    ENV=$e
                    ;;
                *)
                    usage
                    ;;
            esac
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ] ; then
    usage
fi

echo "Environment : $ENV" 

echo "Start building AWS Monitor for MoC WebService Py..."
docker image build  -t aws_api_webservice_${ENV} -f ./Dockerfile.webservice .
echo "Build done! Exiting!"


