### to build the Docker container inside the dir
/home/enrico/columbia/app/micro-local
docker build -t diagnosticator:0.1 .

### to start a MySQL server
docker run --name mysql -d \
    -e MYSQL_RANDOM_ROOT_PASSWORD=yes \
    -e MYSQL_DATABASE=diagnosticator \
    -e MYSQL_USER=diagnosticator \
    -e MYSQL_PASSWORD="diagnosticator" \
    mysql:latest

#   I can also run my homemade image with the export already included
docker run --name mysql -d mysql-diagnosticator:0.1

### to start a REDIS server
docker run --name redis -d -p 6379:6379 redis:latest

### to start a MongoDB server (mirror port 27017 to local 27018 so does NOT interfere with local mongodb)
#   I can also specify user and pwd with
#     -e MONGO_INITDB_ROOT_USERNAME=diagnosticator \
#      -e MONGO_INITDB_ROOT_PASSWORD=diagnosticator \
#   and then put MONGO_URL=mongodb://diagnosticator:diagnosticator@mongo-server:27017/ in diagnosticator docker
docker run --name mongo -d -p 27018:27017 \
      mongo:latest


docker stop diagnosticator && docker rm diagnosticator

### to run diagnosticator container
docker build -t diagnosticator:0.1 . && \
docker run --name diagnosticator --rm \
           --net=bridge \
           -p 3000:5000 \
           -e SERVER_ADDRESS=http://diagnosticator-server:7000 \
           -e TOKEN_EXP_SEC=3600 \
           -e TOKEN_RESTORE_EXP_SEC=300 \
           -e REDIS_URL=redis://redis-server:6379 \
           diagnosticator:0.3

### to run REDIS server
docker run --name rq-worker --rm \
          --link mysql:dbserver \
          --link redis:redis-server \
          -e DATABASE_URL=mysql+pymysql://diagnosticator:diagnosticator@dbserver/diagnosticator \
          -e REDIS_URL=redis://redis-server:6379/0 \
          --entrypoint venv/bin/rq \
          diagnosticator:0.3 worker \
          -u redis://redis-server:6379/0 diagnosticator-tasks





### merged (development)
docker build -t diagnosticator:0.1 . &&  docker run --name rq-worker --rm     --link mysql:dbserver --link redis:redis-server     -e DATABASE_URL=mysql+pymysql://diagnosticator:diagnosticator@dbserver/diagnosticator     -e REDIS_URL=redis://redis-server:6379/0     --entrypoint venv/bin/rq     diagnosticator:0.1 worker -u redis://redis-server:6379/0 diagnosticator-tasks



### I have to build diagnosticator-server too
cd /home/enrico/columbia/app/diagnosticator-server-01
docker build -t diagnosticator-server:0.1 .
docker run --name diagnosticator-server --rm \
           --net=bridge \
           -p 7000:5000 \
            -e MAIL_SERVER="smtp.gmail.com" \
            -e MAIL_PORT=587 \
            -e MAIL_USE_TLS=1 \
            -e MAIL_USERNAME="diagnosticator.edu" \
            -e MAIL_PASSWORD="MZpq9731" \
            -e VARIANT_DB="SQL" \
            -e TOKEN_EXP_SEC=3600 \
            -e TOKEN_RESTORE_EXP_SEC=300 \
            -e LOG_TO_STDOUT=True \
           diagnosticator-server:0.1



docker build -t diagnosticator:0.3 .




############### create a new docker network
docker network create --driver bridge diagnosticator-net

docker run --name mysql-dx-server --rm \
    --network diagnosticator-net \
    --dns 8.8.8.8 \
    --cap-add=sys_nice \
    -e MYSQL_RANDOM_ROOT_PASSWORD=yes \
    -e MYSQL_DATABASE=diagnosticator \
    -e MYSQL_USER=diagnosticator \
    -e MYSQL_PASSWORD="diagnosticator" \
    mysql:latest

docker run --name mysql-dx-local -d --rm \
    --network diagnosticator-net \
    --dns 8.8.8.8 \
    --cap-add=sys_nice \
    -e MYSQL_RANDOM_ROOT_PASSWORD=yes \
    -e MYSQL_DATABASE=diagnosticator \
    -e MYSQL_USER=diagnosticator \
    -e MYSQL_PASSWORD="diagnosticator" \
    mysql:latest


docker run --name diagnosticator-server --rm \
           --network diagnosticator-net \
           --dns 8.8.8.8 \
           -p 7000:5000 \
            -e DATABASE_URL=mysql+pymysql://diagnosticator:diagnosticator@mysql-dx-server/diagnosticator \
            -e MAIL_SERVER="smtp.gmail.com" \
            -e MAIL_PORT=587 \
            -e MAIL_USE_TLS=1 \
            -e MAIL_USERNAME="diagnosticator.edu" \
            -e MAIL_PASSWORD="MZpq9731" \
            -e VARIANT_DB="SQL" \
            -e TOKEN_EXP_SEC=3600 \
            -e TOKEN_RESTORE_EXP_SEC=300 \
            -e LOG_TO_STDOUT=True \
           diagnosticator-server:0.1


docker run --name diagnosticator --rm \
          --network diagnosticator-net \
          --dns 8.8.8.8 \
          -p 3000:5000 \
          -e SERVER_ADDRESS=http://172.19.0.1:7000 \
          -e TOKEN_EXP_SEC=3600 \
          -e TOKEN_RESTORE_EXP_SEC=300 \
          -e REDIS_URL=redis://redis-01:6379 \
          -e DATABASE_URL=mysql+pymysql://diagnosticator:diagnosticator@mysql-dx-local/diagnosticator \
          diagnosticator:0.3

docker run --name redis-01 --rm \
            --network diagnosticator-net \
            --dns 8.8.8.8 \
            -d -p 6377:6379 redis:latest

docker run --name rq-worker --rm \
          --network diagnosticator-net \
          --dns 8.8.8.8 \
          -e DATABASE_URL=mysql+pymysql://diagnosticator:diagnosticator@mysql-dx-local/diagnosticator \
          -e REDIS_URL=redis://redis-01:6379/0 \
          --entrypoint venv/bin/rq \
          diagnosticator:0.3 worker \
          -u redis://redis-01:6379/0 diagnosticator-tasks





docker stop \
      diagnosticator \
      diagnosticator-server \
      mysql-dx-server \
      mysql-dx-local \
      redis-01 \
      rq-worker



docker rm \
      diagnosticator \
      diagnosticator-server \
      mysql-dx-server \
      mysql-dx-local \
      redis-01 \
      rq-worker








### ex. with alpine
docker run -dit --name alpine1 --dns 8.8.8.8 --network diagnosticator-net alpine ash
docker run -dit --name alpine2 --network diagnosticator-net alpine ash
docker run -dit --rm --name alpine3 alpine ash
docker run -dit --name alpine4 --network diagnosticator-net alpine ash
docker network connect bridge alpine4


docker network inspect bridge
docker network inspect diagnosticator-net

docker container attach alpine1


docker container stop alpine1 alpine2 alpine3 alpine4
docker container rm alpine1 alpine2 alpine3 alpine4
docker network rm diagnosticator-net
### ENDc
