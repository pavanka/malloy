#! /bin/bash
set -e

rm -rf .tmp
mkdir .tmp

# run docker
SCRIPTDIR=$(cd $(dirname $0); pwd)
DATADIR=$(dirname $SCRIPTDIR)/data/mysql
docker run -p 3306:3306 -d -v $DATADIR:/init_data --name mysql-malloy -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -d mysql:8.4.2

# wait for server to start
counter=0
echo -n Starting Docker ...
while ! docker logs mysql-malloy 2>&1 | grep -q "mysqld: ready for connections"
do
  sleep 10
  counter=$((counter+1))
  # if doesn't start after 2 minutes, output logs and kill process
  if [ $counter -eq 120 ]
  then
    docker logs mysql-malloy >& ./.tmp/mysql-malloy.logs
    docker rm -f mysql-malloy
    echo "MySQL did not start successfully, check .tmp/mysql-malloy.logs"
    exit 1
    break
  fi
  echo -n ...
done

# load the test data.
echo
echo Loading Test Data
docker exec mysql-malloy cp /init_data/malloytest.mysql.gz /tmp
docker exec mysql-malloy gunzip /tmp/malloytest.mysql.gz
docker exec mysql-malloy mysql -P3306 -h127.0.0.1 -uroot -e 'drop database if exists malloytest; create database malloytest; use malloytest; source /tmp/malloytest.mysql;'

echo "MySQL running on port 3306"
