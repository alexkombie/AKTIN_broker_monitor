#! /bin/bash

set -euo pipefail

readonly WHI='\033[0m'
readonly RED='\e[1;31m'
readonly ORA='\e[0;33m'
readonly YEL='\e[1;33m'
readonly GRE='\e[0;32m'

readonly ROOT_DIR=$(dirname $(cd ..; pwd))

echo -e "${YEL} Build the docker-compose stack ${WHI}"
docker-compose -f docker/docker-compose.yml up -d --force-recreate --build

echo -e "${YEL} Copy python scripts from repository to python container${WHI}"
docker cp $ROOT_DIR/src/node_to_csv.py python:/opt/
docker cp $ROOT_DIR/src/csv_to_confluence.py python:/opt/
docker cp $ROOT_DIR/src/common.py python:/opt/
docker cp $ROOT_DIR/src/resources/template_page.html python:/opt/

echo -e "${YEL} Run python unit tests ${WHI}"
docker exec python pytest test_BrokerNodeConnection.py
docker exec python pytest test_ConfluenceNodeMapper.py
docker exec python pytest test_SingletonMeta.py
docker exec python pytest test_NodeInfoFetcher.py
docker exec python pytest test_NodeErrorFetcher.py
docker exec python pytest test_BrokerNodeResourceFetcher.py
docker exec python pytest test_BrokerNodeFetcherManager.py
docker exec python pytest test_TemplatePageCSVInfoWriter.py

LIST_CONTAINER=( broker-server python )
echo -e "${YEL} Stop all container ${WHI}"
for container in ${LIST_CONTAINER[*]}; do
  docker stop $container
done

echo -e "${YEL} Remove all container ${WHI}"
for container in ${LIST_CONTAINER[*]}; do
  docker rm $container
done

echo -e "${YEL} Remove all images ${WHI}"
for container in ${LIST_CONTAINER[*]}; do
  docker image rm $container
done
