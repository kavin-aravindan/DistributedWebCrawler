#!/bin/bash

set -e

BASE_DIR="./mongo_cluster"
CONFIGSVR_PORT_BASE=26050
SHARD_PORT_BASE=27018
MONGOS_PORT=27017

# Clean up old cluster
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"

echo "Starting Config Servers..."
for i in 1 2 3; do
    mkdir -p "$BASE_DIR/config$i"
    mongod --configsvr --replSet configReplSet --port $(($CONFIGSVR_PORT_BASE + i)) \
        --dbpath "$BASE_DIR/config$i" --fork --logpath "$BASE_DIR/config$i/mongod.log"
done

sleep 3

echo "Initiating Config Server Replica Set..."
mongo --port $(($CONFIGSVR_PORT_BASE + 1)) --eval '
rs.initiate({
    _id: "configReplSet",
    configsvr: true,
    members: [
        {_id: 0, host: "localhost:26051"},
        {_id: 1, host: "localhost:26052"},
        {_id: 2, host: "localhost:26053"}
    ]
})
'
sleep 5

echo "Starting Shards..."
for i in 1 2; do
    mkdir -p "$BASE_DIR/shard$i"
    mongod --shardsvr --replSet shardReplSet$i --port $(($SHARD_PORT_BASE + i)) \
        --dbpath "$BASE_DIR/shard$i" --fork --logpath "$BASE_DIR/shard$i/mongod.log"
done

sleep 3

echo "Initiating Shard Replica Sets..."
for i in 1 2; do
mongo --port $(($SHARD_PORT_BASE + i)) --eval "
rs.initiate({
    _id: \"shardReplSet$i\",
    members: [{_id: 0, host: \"localhost:$(($SHARD_PORT_BASE + i))\"}]
})
"
done

sleep 5

echo "Starting mongos router..."
mongos --configdb configReplSet/localhost:26051,localhost:26052,localhost:26053 \
    --port $MONGOS_PORT --fork --logpath "$BASE_DIR/mongos.log"

sleep 5

echo "Adding shards to mongos..."
mongo --port $MONGOS_PORT --eval '
sh.addShard("shardReplSet1/localhost:27019")
sh.addShard("shardReplSet2/localhost:27020")
'

sleep 2

echo "Enabling sharding on database and collection..."
mongo --port $MONGOS_PORT --eval '
sh.enableSharding("webcrawler")
db = db.getSiblingDB("webcrawler")
db.pages.createIndex({ url: 1 })
sh.shardCollection("webcrawler.pages", { url: 1 })
'

echo "MongoDB sharded cluster setup complete."
echo "Connect with: mongo --port 27017"
