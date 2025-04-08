#!/bin/bash

# delete old server files comment out if needed
rm -rf nodes/

trap 'kill $(jobs -p)' SIGINT SIGTERM EXIT

mkdir nodes/
cd nodes/
mkdir -p {7000..7005}
for i in {7000..7005}; do
    cp ../redis.conf $i;
    sed -i "s/^port .*/port $i/" "$i/redis.conf"
    cd $i
    redis-server redis.conf &
    cd ..
    sleep 1
done

echo 'yes' | redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 \
127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
--cluster-replicas 1

wait
