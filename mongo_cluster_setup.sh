#!/bin/bash

set -e

# ======================
# CONFIG
# ======================
BASE_DIR=$(pwd)
DATA_DIR="$BASE_DIR/data"
CONFIG_REPL_SET="configReplSet"
SHARD_REPL_SET="shardReplSet"
MONGOS_PORT=27017

# ======================
# HELPER FUNCTIONS
# ======================
function check_mongo_shell {
    if command -v mongosh &>/dev/null; then
        MONGO_SHELL="mongosh"
    elif command -v mongo &>/dev/null; then
        MONGO_SHELL="mongo"
    else
        echo "Error: Neither 'mongosh' nor 'mongo' command found. Please install MongoDB shell."
        exit 1
    fi
}

function start_mongod {
    local dbpath=$1
    local port=$2
    local logpath=$3
    local replset=$4
    local is_config=$5

    mkdir -p "$dbpath"
    
    if [ "$is_config" = true ]; then
        mongod --port "$port" \
            --dbpath "$dbpath" \
            --replSet "$replset" \
            --configsvr \
            --bind_ip localhost \
            --logpath "$logpath" \
            --fork --logappend
    else
        mongod --port "$port" \
            --dbpath "$dbpath" \
            --replSet "$replset" \
            --shardsvr \
            --bind_ip localhost \
            --logpath "$logpath" \
            --fork --logappend
    fi
}


# ======================
# START CONFIG SERVERS
# ======================
echo "Starting Config Servers..."
start_mongod "$DATA_DIR/config1" 27019 "$DATA_DIR/config1/mongod.log" $CONFIG_REPL_SET true
start_mongod "$DATA_DIR/config2" 27020 "$DATA_DIR/config2/mongod.log" $CONFIG_REPL_SET true
start_mongod "$DATA_DIR/config3" 27021 "$DATA_DIR/config3/mongod.log" $CONFIG_REPL_SET true

# Give a moment to stabilize
sleep 2

# ======================
# INIT CONFIG REPLICA SET
# ======================
check_mongo_shell
echo "Initiating Config Replica Set..."

$MONGO_SHELL --port 27019 <<EOF
rs.initiate({
  _id: "$CONFIG_REPL_SET",
  configsvr: true,
  members: [
    { _id: 0, host: "localhost:27019" },
    { _id: 1, host: "localhost:27020" },
    { _id: 2, host: "localhost:27021" }
  ]
})
EOF

sleep 5

# ======================
# START SHARD SERVERS
# ======================
echo "Starting Shard Replica Set..."
start_mongod "$DATA_DIR/shard1" 27022 "$DATA_DIR/shard1/mongod.log" $SHARD_REPL_SET false
start_mongod "$DATA_DIR/shard2" 27023 "$DATA_DIR/shard2/mongod.log" $SHARD_REPL_SET false
start_mongod "$DATA_DIR/shard3" 27024 "$DATA_DIR/shard3/mongod.log" $SHARD_REPL_SET false

sleep 2

echo "Initiating Shard Replica Set..."
$MONGO_SHELL --port 27022 <<EOF
rs.initiate({
  _id: "$SHARD_REPL_SET",
  members: [
    { _id: 0, host: "localhost:27022" },
    { _id: 1, host: "localhost:27023" },
    { _id: 2, host: "localhost:27024" }
  ]
})
EOF

sleep 5

# ======================
# START MONGOS
# ======================
echo "Starting mongos..."
mongos --configdb $CONFIG_REPL_SET/localhost:27019,localhost:27020,localhost:27021 \
       --port $MONGOS_PORT \
       --bind_ip localhost \
       --logpath "$DATA_DIR/mongos.log" \
       --fork --logappend

sleep 3

# ======================
# ADD SHARD TO CLUSTER
# ======================
echo "🔗 Adding shard to mongos..."
$MONGO_SHELL --port $MONGOS_PORT <<EOF
sh.addShard("$SHARD_REPL_SET/localhost:27022,localhost:27023,localhost:27024")
EOF

echo "MongoDB Cluster Setup Complete!"
