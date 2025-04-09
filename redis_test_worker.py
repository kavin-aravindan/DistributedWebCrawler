import time
import requests
from redis.cluster import RedisCluster 
from bs4 import BeautifulSoup

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
        if href.startswith('/'):
            href = url + href
        links.add(href)
    return links

def extract_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.get_text()
    return content

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
    for url in links:
        add_url(url)

    time.sleep(1)

# empty url_set for testing
rc.delete("url_set")
rc.delete("url_queue")
