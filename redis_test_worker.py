import time
from redis.cluster import RedisCluster
from pymongo import MongoClient
from scraper import scrape_page
from redis_common import add_url, get_url

# MongoDB setup
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client['webcrawler']
collection = db['pages']

# Redis Cluster setup
startup_nodes = [{"host": "localhost", "port": "7000"}]
rc = RedisCluster(host="localhost", port=7000, decode_responses=False)
rc.ping()
print("Cluster running")

# Constants
BLOCKING_TIMEOUT = 3
SCRAPE_THRESHOLD_SECONDS = 3600  # e.g. 1 hour

url_queue = '{crawler}url_queue'
url_set = '{crawler}url_set'
processing_queue = '{crawler}processing_queue'




def store_in_db(url, content):
    try:
        result = collection.update_one(
            {'url': url},
            {"$set": {
                'content': content,
                'last_scraped': time.time()
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

    url = rc.brpoplpush(url_queue, processing_queue, timeout=BLOCKING_TIMEOUT)
    if url is None:
        print("No URLs in the queue")
        time.sleep(1)
        continue

    url = url.decode('utf-8')
    print(f"Checking {url}...")

    # Check last scrape time
    existing_doc = collection.find_one({'url': url})
    if existing_doc and 'last_scraped' in existing_doc:
        last_scraped = existing_doc['last_scraped']
        now = time.time()
        time_diff = now - last_scraped
        if time_diff < SCRAPE_THRESHOLD_SECONDS:
            print(f"Already scraped recently: {url} ({int(time_diff)} seconds ago)")
            rc.lrem(processing_queue, 1, url)
            time.sleep(0.1)
            continue

    print(f"Scraping {url}")

    links, content = scrape_page(url)

    if content:
        store_in_db(url, content)

    for link in links:
        add_url(rc, link)

    rc.lrem(processing_queue, 1, url)
    time.sleep(0.1)

# empty url_set for testing
# rc.delete("url_set")
# rc.delete("url_queue")

#15:39:43
