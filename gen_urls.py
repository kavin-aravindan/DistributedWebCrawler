import time
from redis.cluster import RedisCluster
from pymongo import MongoClient
from datetime import datetime
from scraper import scrape_page
from redis_common import add_url, get_url

rc = RedisCluster(host="localhost", port=7000, decode_responses=False)
rc.ping()
print("Cluster running")

n = 1000
i = 0
urls = []
for i in range(n):
    url = get_url(rc)
    url = url.decode('utf-8')
    urls.append(url)
    links, content = scrape_page(url)
    for url in links:
        add_url(rc, url)
        
# write urls to file
with open('urls.txt', 'w') as f:
    for url in urls:
        f.write(f"{url}\n")