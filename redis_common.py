from redis.cluster import RedisCluster 
from urllib.parse import urlparse, parse_qsl

url_queue = '{crawler}url_queue'
url_set = '{crawler}url_set'
processing_queue = '{crawler}processing_queue'

# --- Whitelist Configuration ---
ALLOWED_DOMAIN = "en.wikipedia.org"
ARTICLE_PATH_PREFIX = "/wiki/"
# Query parameters to reject if present
BLACKLISTED_QUERY_PARAMS = {'action', 'diff', 'oldid'}
# --- End Whitelist Configuration ---

def check_wikipage_valid(url_str):
    """Checks if a URL matches the whitelist criteria for a Wikipedia article."""
    try:
        parsed = urlparse(url_str)
        if not parsed.netloc.endswith(ALLOWED_DOMAIN):
            return False

        if not parsed.path.startswith(ARTICLE_PATH_PREFIX):
            return False

        # 3. Check for Namespace Prefix (colon) immediately after /wiki/
        path_part_after_wiki = parsed.path[len(ARTICLE_PATH_PREFIX):]
        if ':' in path_part_after_wiki:
            # print(f"DEBUG: Rejecting {url_str} - Contains colon in path ({path_part_after_wiki})")
            return False

        # 4. Check for Blacklisted Query Parameters
        query_params = dict(parse_qsl(parsed.query))
        for param in query_params:
            if param in BLACKLISTED_QUERY_PARAMS:
                # print(f"DEBUG: Rejecting {url_str} - Blacklisted query param ({param})")
                return False

        # If all checks pass, it's likely an article URL
        return True

    except Exception as e:
        # Handle potential errors during URL parsing
        print(f"Error parsing URL '{url_str}': {e}")
        return False
    

def get_url(rc):
    url = rc.lpop(url_queue)
    if url is None:
        print("No urls in the queue")
        return None
    else:
        print(f"Got {url} from the queue")
        return url
    
def add_url(rc, url):
    # check if its a valid wikipedia url
    if not check_wikipage_valid(url):
        return 0 
    
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