import time
from redis.cluster import RedisCluster 

startup_nodes = [
    {"host": "localhost", "port": "7000"}
    ]


rc = RedisCluster(host="localhost", port=7000)
rc.ping()
print("Cluster running")

def get_url():
    url = rc.lpop("url_queue")
    if url is None:
        print("No urls in the queue")
        return None
    else:
        print(f"Got {url} from the queue")
        return url
    
while True:
    url = get_url()
    if url is None:
        break
    # do something with the url
    print(f"Processing {url}")
    time.sleep(1)
    
# empty url_set for testing
rc.delete("url_set")
rc.delete("url_queue")