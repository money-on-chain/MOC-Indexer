#!/bin/bash

# exit as soon as an error happen
set -e 

# login into aws ecr
#$(aws ecr get-login --no-include-email --region us-west-1)
aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 551471957915.dkr.ecr.us-west-1.amazonaws.com


usage() { echo "Usage: $0 -e <environment>" 1>&2; exit 1; }

# validate the environments
while getopts ":e:t:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "alpha-testnet" || e == "testnet" || e == "mainnet" )) || usage
            case $e in
                alpha-testnet)
                    ;;
                testnet)      
                    ;;
                mainnet)
                    ;;
                *)
                    usage
                    ;;
            esac
            ;;

        t) 
            t=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ]; then
    usage
fi

# we tag and push to ECR
#docker tag aws_api_webservice_$e:latest 551471957915.dkr.ecr.us-west-1.amazonaws.com/aws_api_web_service_$e:latest
#docker push 551471957915.dkr.ecr.us-west-1.amazonaws.com/aws_api_webservice_$e:latest
