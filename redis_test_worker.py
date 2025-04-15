import time
from redis.cluster import RedisCluster
from pymongo import MongoClient
from datetime import datetime
from scraper import scrape_page
from redis_common import add_url, get_url

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

    time.sleep(0.1)

# empty url_set for testing
# rc.delete("url_set")
# rc.delete("url_queue")
