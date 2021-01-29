# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment>" 1>&2; exit 1; }

while getopts ":e:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
            (( e == "ec2_alphatestnet" || e=="ec2_testnet" || e=="ec2_testnet_historic" || e=="ec2_mainnet" || e=="ec2_mainnet_historic" || e=="ec2_rdoc_alphatestnet" || e=="ec2_rdoc_testnet" || e=="ec2_rdoc_testnet_historic" || e=="ec2_rdoc_mainnet" || e=="ec2_rdoc_mainnet_historic")) || usage
            case $e in
                ec2_alphatestnet)
                    ENV=$e
                    ;;
                ec2_testnet)
                    ENV=$e
                    ;;
                ec2_testnet_historic)
                    ENV=$e
                    ;;
                ec2_mainnet)
                    ENV=$e
                    ;;
                ec2_mainnet_historic)
                    ENV=$e
                    ;;
                ec2_rdoc_alphatestnet)
                    ENV=$e
                    ;;
                ec2_rdoc_testnet)
                    ENV=$e
                    ;;
                ec2_rdoc_testnet_historic)
                    ENV=$e
                    ;;
                ec2_rdoc_mainnet)
                    ENV=$e
                    ;;
                ec2_rdoc_mainnet_historic)
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

if [ -z "${e}" ]; then
    usage
fi

REGION="us-west-1" # us-west-1

# login into aws ecr
$(aws ecr get-login --no-include-email --region $REGION)

docker tag moc_indexer_$ENV:latest 551471957915.dkr.ecr.$REGION.amazonaws.com/moc_indexer_$ENV:latest
docker push 551471957915.dkr.ecr.$REGION.amazonaws.com/moc_indexer_$ENV:latest