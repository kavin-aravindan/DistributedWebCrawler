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
    
print("Master: Checking initial queue state...")
initial_len = rc.llen('{crawler}url_queue')
print(f"Master: Length of {url_queue} = {initial_len}")
is_member = rc.sismember('{crawler}url_set', 'https://books.toscrape.com/index.html'.encode('utf-8'))
print(f"Master: Is initial URL in {url_set}? {is_member}")
if initial_len == 0:
    print("Master WARNING: URL Queue is empty immediately after adding!")
    
# keep checking for failed links
while True:
    item = rc.rpoplpush(processing_queue, url_queue)
    if item:
        print(f'requeueued {item.decode("utf-8")}')
    time.sleep(30)