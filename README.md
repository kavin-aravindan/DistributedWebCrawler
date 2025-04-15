# Redis setup
```
sudo apt install redis-server 
redis-server --version
```

Make the following changes to 'etc/redis/redis.conf
1. comment out 
```
bind 127.0.0.1 ::1
```
2. change 'supervised no' to 'supervised systemd'

Restart redis using and check if it is online
```
sudo systemctl disable redis-server 
sudo systemctl restart redis-server
redis-cli ping
```

You should receive a 'PONG' in response

Install the python packages (use a venv)

```
pip install -r requirements.txt
```

run create_redis.sh to generate 6 servers(3 original, replicas) on ports 7000-7005

Install mongoDB: https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/

Install java 
```
sudo apt install openjdk-11-jdk
```

To start Mongo clusters for sharding, run the bash script:
```
chmod +x mongo_cluster_setup.sh
./mongo_cluster_setup.sh
```

To restart the mongo clusters, run 
``` 
pkill mongod
pkill mongos
rm -rf mongo_cluster
```

Install spark:
```
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
sudo mv spark-3.5.5-bin-hadoop3/ ~/spark
tar -xvf spark-3.5.5-bin-hadoop3.tgz
```

To your .bashrc file add
```
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

Activate changes using `source ~/.bashrc`

Create `spark-env.sh` in `$SPARK_HOME/conf/`
Add the lines
```
SPARK_MASTER_HOST=127.0.0.1
SPARK_WORKER_INSTANCES=4
SPARK_WORKER_CORES=1
SPARK_WORKER_MEMORY=1g
```
Depending on your system specifications you can change the number of worker instances, cores per worker, and memory per worker. This will change the number of workers spawned on one device.

To set up the cluster run 
```
chmod +x create_spark_cluster.sh
./create_spark_cluster.sh
```

exiting the script will kill the workers and master.

# How to run
pkill mongod
pkill mongos
rm -rf data
chmod +x mongo_cluster_setup.sh
./mongo_cluster_setup.sh
chmod +x create_spark_cluster.sh
./create_spark_cluster.sh
chmod +x create_redis.sh
./create_redis.sh
run master
run worker(s)
run search
<!-- To stop the master and workers use
```
~/spark/sbin/stop-master.sh
~/spark/sbin/stop-worker.sh
``` -->
