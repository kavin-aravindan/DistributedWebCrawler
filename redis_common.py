from redis.cluster import RedisCluster 

url_queue = '{crawler}url_queue'
url_set = '{crawler}url_set'
processing_queue = '{crawler}processing_queue'

def get_url(rc):
    url = rc.lpop(url_queue)
    if url is None:
        print("No urls in the queue")
        return None
    else:
        print(f"Got {url} from the queue")
        return url
    
def add_url(rc, url):
    # encode the url
    url = url.encode('utf-8')
    # check if url is already in the url set
    if rc.sismember(url_set, url):
        # print(f"{url} is already in the url set")
        return -1
    # check if url is already being processed
    rc.sadd(url_set, url)
    rc.rpush(url_queue, url)
    print(f"Added {url} to the url set and queue")
    return 1