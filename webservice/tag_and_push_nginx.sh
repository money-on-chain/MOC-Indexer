# login into aws ecr
#$(aws ecr get-login --no-include-email --region us-west-1)
set -e 

aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 551471957915.dkr.ecr.us-west-1.amazonaws.com

# we tag and push to ECR
docker tag nginx_api_ipfs:latest 551471957915.dkr.ecr.us-west-1.amazonaws.com/nginx_api_ipfs:latest
docker push 551471957915.dkr.ecr.us-west-1.amazonaws.com/nginx_api_ipfs:latest
