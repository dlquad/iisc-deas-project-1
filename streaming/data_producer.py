import time
import json
import requests
from bs4 import BeautifulSoup
from collections import deque
from urllib.parse import urljoin, urlparse

from kafka_manager import producer
from utils import KafkaTopics, logger

KAFKA_TOPIC = KafkaTopics.SCRAPED_DATA.value
SEED_URL = "https://www.nytimes.com/"

visited_urls = set()
urls_to_visit = deque([SEED_URL])

def is_valid_url(url, base_domain):
    parsed = urlparse(url)
    return bool(parsed.netloc) and (base_domain in parsed.netloc)

def scrape_page(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Python Spider)'}
        response = requests.get(url, headers=headers, timeout=5)
        
        # If not text/html (e.g. it's an image or PDF), skip
        if "text/html" not in response.headers.get("Content-Type", ""):
            return None, []

        soup = BeautifulSoup(response.text, 'html.parser')
        
        title = soup.find('h1').get_text(strip=True) if soup.find('h1') else "No Title"
        paragraphs = [p.get_text(strip=True) for p in soup.find_all('p')]
        content = " ".join(paragraphs)

        article_data = {
            "source": url,
            "text": content[:5000] + "...",
            "title": title
        }

        new_links = []
        base_domain = urlparse(url).netloc
        
        for link in soup.find_all('a', href=True):
            full_url = urljoin(url, link['href'])
            if is_valid_url(full_url, base_domain) and full_url not in visited_urls:
                new_links.append(full_url)

        return article_data, new_links

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None, []

def start_producer():
    """Start the web scraping producer."""
    logger.info(f"Starting spider at {SEED_URL}...")

    while urls_to_visit:
        current_url = urls_to_visit.popleft()
        
        if current_url in visited_urls:
            continue

        logger.info(f"Processing: {current_url}")
        
        data, found_links = scrape_page(current_url)
        visited_urls.add(current_url)

        if data and len(data['text']) > 100: # Filter out thin content/nav pages
            producer.produce(topic=KAFKA_TOPIC, value=json.dumps(data).encode('utf-8'))
        
        for link in found_links:
            if link not in visited_urls:
                urls_to_visit.append(link)

        time.sleep(10)

if __name__ == "__main__":
    start_producer()