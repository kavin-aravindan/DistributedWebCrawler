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

