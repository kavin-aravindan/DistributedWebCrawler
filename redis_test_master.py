from redis.cluster import RedisCluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from redis_common import add_url, get_url

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


def search(search_term, inverted_index_file):
    # Load index
    index = spark.sparkContext.textFile(inverted_index_file)
    matches = index.filter(lambda x: search_term in x).collect()

    for line in matches:
        print(line)


# urls = [
#     "https://books.toscrape.com/index.html"
# ]
# for url in urls:
#     add_url(rc, url)
    
# start an interactive shell
while True:
    search_term = input("Enter a search term (or 'exit' to quit): ")
    if search_term.lower() == "exit":
        break
    search(search_term, "inverted_index")
