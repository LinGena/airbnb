import requests
from utils.db import Db
import gzip
from io import BytesIO
from bs4 import BeautifulSoup
import logging


class GetLinksSitemap(Db):
    def __init__(self):
        super().__init__()

    def run(self):
        for sitemap_link in self.get_sitemap_index_links():
            try:
                print('----', sitemap_link)
                self.get_sitemap(sitemap_link)
            except Exception as ex:
                logging.error(ex)

    def get_sitemap_index_links(self) -> list:
        index_url = 'https://www.airbnb.ru/sitemap-master-index.xml.gz'
        response = requests.get(index_url, timeout=30)
        response.raise_for_status()
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gzipped_file:
            content = gzipped_file.read()

        soup = BeautifulSoup(content, 'xml')
        loc_tags = soup.find_all('loc')
        links = []
        for loc in loc_tags:
            sitemap_link = loc.get_text(strip=True)
            if sitemap_link and 'sitemap-homes-urls' in sitemap_link:
                links.append(sitemap_link)
        return links
        
    def get_sitemap(self, sitemap_url: str) -> None:
        response = requests.get(sitemap_url, timeout=30)
        response.raise_for_status()
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gzipped_file:
            content = gzipped_file.read()
        soup = BeautifulSoup(content, 'xml')
        loc_tags = soup.find_all('loc')
        loc_urls = [loc.get_text() for loc in loc_tags]
        rows: list[list[str]] = []
        for url in loc_urls:
            if '/rooms/' not in url:
                continue
            link = str(url).replace('airbnb.ru', 'airbnb.com')
            rows.append([link])
        if not rows:
            return
        self.insert_many(
            'INSERT IGNORE INTO airbnb (link) VALUES (%s)',
            rows,
            batch_size=500,
        )