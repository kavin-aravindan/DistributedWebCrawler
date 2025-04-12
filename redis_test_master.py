from redis.cluster import RedisCluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DistributedWebCrawler") \
    .getOrCreate()

startup_nodes = [
    {"host": "localhost", "port": "7000"}
    ]


rc = RedisCluster(host="localhost", port=7000, decode_responses=True)
print(rc.get_nodes())
rc.ping()
print("Cluster running")


def add_url(url):
    # check if url is already in the url set
    if rc.sismember("url_set", url):
        print(f"{url} is already in the url set")
        return -1
    # check if url is already being processed
    rc.sadd("url_set", url)
    rc.rpush("url_queue", url)
    print(f"Added {url} to the url set and queue")
    return 1

def search(search_term, inverted_index_file):
    # Load index
    index = spark.sparkContext.textFile(inverted_index_file)
    matches = index.filter(lambda x: search_term in x).collect()

    for line in matches:
        print(line)


urls = [
    "www.google.com",
    "www.github.com",
    "www.reddit.com",
    "www.stackoverflow.com",
    "www.wikipedia.org"
]
for url in urls:
    add_url(url)