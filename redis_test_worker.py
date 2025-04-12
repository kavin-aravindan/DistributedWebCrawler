import time
import requests
from redis.cluster import RedisCluster 
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from pymongo import MongoClient
from datetime import datetime

mongo_client = MongoClient('localhost', 27017)
db = mongo_client['webcrawler']
collection = db['pages']

startup_nodes = [
    {"host": "localhost", "port": "7000"}
    ]


rc = RedisCluster(host="localhost", port=7000)
rc.ping()
print("Cluster running")

def scrape_page(url):
    if not url.startswith("http"):
        url = "http://" + url

    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Successfully scraped {url}")
            return extract_links(response.text, url), extract_content(response.text)
        else:
            print(f"Failed to scrape {url}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return None

def extract_links(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(url, href)
        if full_url.startswith(url) or full_url.startswith('http'):
            pass
        else:
            full_url = url + href

        # clean the full_url
        full_url = normalise_url(full_url)
        links.add(full_url)

    return links


def normalise_url(url):
    parsed_url = urlparse(url)

    scheme = parsed_url.scheme.lower()
    netloc = parsed_url.netloc.lower()

    path = parsed_url.path.rstrip('/')
    fragment = ''

    query_params = parse_qsl(parsed_url.query)
    new_query_params = [(k, v) for k, v in query_params if not k.startswith('utm_')]
    new_query = urlencode(new_query_params)

    normalised_url = urlunparse((scheme, netloc, path, '', new_query, fragment))

    return normalised_url


def extract_content(html):
    soup = BeautifulSoup(html, 'html.parser')

    content = [soup.get_text(separator=" ", strip=True)]

    # image alt text
    image_alt_text = []
    for img in soup.find_all('img'):
        alt_text = img.get('alt')
        if alt_text:
            image_alt_text.append(alt_text)
    image_alt_text = " ".join(image_alt_text)
    content.append(image_alt_text)

    # meta description
    meta_description = ""
    meta_tag = soup.find('meta', attrs={'name': 'description'})
    if meta_tag:
        meta_description = meta_tag.get('content')
        content.append(meta_description)
    else:
        meta_tag = soup.find('meta', attrs={'property': 'og:description'})
        if meta_tag:
            meta_description = meta_tag.get('content')
            content.append(meta_description)

    content = " ".join(content)
    # remove newlines and extra spaces
    content = content.replace("\n", " ").replace("\r", " ")

    return content

def store_in_db(url, content):
    collection.update_one(
        {'url': url},
        {"$set": {
            'content': content,
            'last_scraped': datetime.utcnow()
        }},
        upsert=True
    )

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
    
    links, content = scrape_page(url)

    if content:
        store_in_db(url, content)

    for url in links:
        add_url(url)

    time.sleep(1)

# empty url_set for testing
rc.delete("url_set")
rc.delete("url_queue")
