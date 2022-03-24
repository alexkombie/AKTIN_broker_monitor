@echo off

if not defined in_subprocess (cmd /k set in_subprocess=y ^& %0 %*) & exit )
for /F %%a in ('echo prompt $E ^| cmd') do @set "ESC=%%a"
for %%A in  (%~dp0\..\.) do set RootDirPath=%%~dpA

echo %ESC%[33m Build the docker-compose stack %ESC%[0m
docker-compose -f docker/docker-compose.yml up -d --force-recreate --build

echo %ESC%[33m Copy python scripts from repository to python container %ESC%[0m
docker cp %RootDirPath%/src/node_to_csv.py python:/opt/
docker cp %RootDirPath%/src/csv_to_confluence.py python:/opt/
docker cp %RootDirPath%/src/common.py python:/opt/

echo %ESC%[33m Run python unit tests %ESC%[0m
docker exec python pytest test_BrokerNodeConnection.py
docker exec python pytest test_ConfluenceNodeMapper.py
docker exec python pytest test_SingletonMeta.py
docker exec python pytest test_NodeInfoFetcher.py
docker exec python pytest test_NodeErrorFetcher.py
docker exec python pytest test_BrokerNodeResourceFetcher.py
docker exec python pytest test_BrokerNodeFetcherManager.py

set ListContainer=broker-server python
echo %ESC%[33m Stop all container %ESC%[0m
(for %%a in (%ListContainer%) do (
    docker stop %%a
))

echo %ESC%[33m Remove all container %ESC%[0m
(for %%a in (%ListContainer%) do (
    docker rm %%a
))

echo %ESC%[33m Remove all images %ESC%[0m
(for %%a in (%ListContainer%) do (
    docker image rm %%a
))

pause
