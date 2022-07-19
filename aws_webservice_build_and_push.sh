# exit as soon as an error happen
set -e

usage() { echo "Usage: $0 -e <environment> -c <config file> -i <aws id> -r <aws region>" 1>&2; exit 1; }

while getopts ":e:c:i:r:" o; do
    case "${o}" in
        e)
            e=${OPTARG}
            (( e == "bnb-testnet" || e == "alpha-testnet" || e=="testnet" ||  e=="mainnet" || e=="rdoc-alpha-testnet" || e=="rdoc-testnet" ||  e=="rdoc-mainnet")) || usage
            case $e in
                bnb-testnet)
                    ENV=$e
                    ;;
                alpha-testnet)
                    ENV=$e
                    ;;
                testnet)
                    ENV=$e
                    ;;
                mainnet)
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
        c)
            c=${OPTARG}
            CONFIG_FILE=$c
            ;;
        i)
            i=${OPTARG}
            AWS_ID=$i
            ;;
        r)
            r=${OPTARG}
            AWS_REGION=$r
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${e}" ] || [ -z "${c}" ] || [ -z "${i}" ] || [ -z "${r}" ]; then
    usage
fi

docker image build -t aws_api_webservice_$ENV -f ./Dockerfile.webservice --build-arg APP_SETTINGS=$CONFIG_FILE .

echo "Build done!"

# login into aws ecr
$(aws ecr get-login --no-include-email --region $AWS_REGION)

echo "Logging to AWS done!"

docker tag aws_api_webservice_$ENV:latest $AWS_ID.dkr.ecr.$AWS_REGION.amazonaws.com/aws_api_webservice_$ENV:latest

docker push $AWS_ID.dkr.ecr.$AWS_REGION.amazonaws.com/aws_api_webservice_$ENV:latest

echo "finish done!"