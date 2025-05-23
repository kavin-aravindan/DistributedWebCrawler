import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import time

# --- Configuration for Politeness ---
REQUEST_COOLDOWN_SECONDS = 5
HEADERS = {
    'User-Agent': 'DistubutedWebCrawler/1.0 (kavin.aravindan@research.iiit.ac.in)'
}
REQUEST_TIMEOUT = 15

def scrape_page(url):
    if not url.startswith("http"):
        url = "http://" + url

    try:
        time.sleep(REQUEST_COOLDOWN_SECONDS)
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
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
