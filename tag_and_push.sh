# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment>" 1>&2; exit 1; }

while getopts ":e:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
             ((e == "moc-alphatestnet" || e == "moc-testnet" || e == "moc-mainnet" || e == "rdoc-mainnet" || e == "rdoc-testnet" || e == "rdoc-alpha-testnet"  )) || usage
            case $e in
                moc-alphatestnet)
                    ENV=$e
                    ;;
                moc-testnet)
                    ENV=$e
                    ;;
                moc-mainnet)
                    ENV=$e
                    ;;
                rdoc-alpha-testnet)
                    ENV=$e
                    ;;
                rdoc-testnet)
                    ENV=$e
                    ;;
                rdoc-mainnet)
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

# login into aws ecr
$(aws ecr get-login --no-include-email --region us-west-1)

docker tag moc_indexer_$ENV:latest 551471957915.dkr.ecr.us-west-1.amazonaws.com/moc_indexer_$ENV:latest
docker push 551471957915.dkr.ecr.us-west-1.amazonaws.com/moc_indexer_$ENV:latest