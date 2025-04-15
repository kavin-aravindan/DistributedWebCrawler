import time
from redis.cluster import RedisCluster
from pymongo import MongoClient
from datetime import datetime
from scraper import scrape_page
from redis_common import add_url, get_url
import os
import csv
import psutil

# mongo_client = MongoClient('localhost', 27017)
# Connect to the mongos router on port 27017.
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client['webcrawler']
collection = db['pages']

startup_nodes = [
    {"host": "localhost", "port": "7000"}
    ]

BLOCKING_TIMEOUT = 3

url_queue = '{crawler}url_queue'
url_set = '{crawler}url_set'
processing_queue = '{crawler}processing_queue'

rc = RedisCluster(host="localhost", port=7000, decode_responses=False)
rc.ping()
print("Cluster running")
urls_counter = 'urls_processed'
pid = os.getpid()
cpu_memory_file = f"metrics/cpu_memory_{pid}.csv"

def store_in_db(url, content):
    try:
        result = collection.update_one(
            {'url': url},  # Shard key
            {"$set": {
                'content': content,
                'last_scraped': datetime.utcnow()
            }},
            upsert=True
        )
        if result.upserted_id:
            print(f"Inserted new page: {url}")
        else:
            print(f"Updated existing page: {url}")
    except Exception as e:
        print(f"MongoDB error storing {url}: {e}")

# Create CSV file with header if it doesn't exist
if not os.path.exists(cpu_memory_file):
    with open(cpu_memory_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'cpu_usage', 'memory_usage'])
prev_time = time.time()

def get_metrics(pid):
    process = psutil.Process(pid)
    cpu_usage = process.cpu_percent(interval=0.3)
    memory_info = process.memory_info()
    memory_usage = memory_info.rss / (1024 * 1024)  # Convert to MB
    return cpu_usage, memory_usage

while True:
    # url = get_url(rc)
    url = rc.brpoplpush(url_queue, processing_queue, timeout=BLOCKING_TIMEOUT)
    if url is None:
        print("No urls in the queue")
        continue
    
    # convert the url to a string
    url = url.decode('utf-8')
    print(f"Scraping {url}")
    
    links, content = scrape_page(url)

    if content:
        store_in_db(url, content)

    for url in links:
        add_url(rc, url)
    
    remove = rc.lrem(processing_queue, 1, url)

    # log the number of urls processed: adds 1
    rc.incr(urls_counter)

    # every 5 seconds, log the cpu and memory usage
    if time.time() - prev_time > 5:
        with open(cpu_memory_file, 'a', newline='') as f:
            writer = csv.writer(f)
            time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            cpu_usage, memory_usage = get_metrics(pid)
            writer.writerow([time_stamp, cpu_usage, memory_usage])
        prev_time = time.time()

    time.sleep(0.1)

# empty url_set for testing
# rc.delete("url_set")
# rc.delete("url_queue")
