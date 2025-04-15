from redis.cluster import RedisCluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from redis_common import add_url, get_url
import re
import ast  
from collections import defaultdict
import time

url_queue = '{crawler}url_queue'
url_set = '{crawler}url_set'
processing_queue = '{crawler}processing_queue'

startup_nodes = [
    {"host": "localhost", "port": "7000"}
    ]

BLOCKING_TIMEOUT = 3

rc = RedisCluster(host="localhost", port=7000, decode_responses=False)
print(rc.get_nodes())
rc.ping()
print("Cluster running")

urls = [
    "https://books.toscrape.com/index.html"
]
for url in urls:
    add_url(rc, url)
    
# keep checking for failed links
while True:
    item = rc.rpoplpush(processing_queue, url_queue)
    if item:
        print(f'requeueued {item.decode("utf-8")}')
    time.sleep(30)