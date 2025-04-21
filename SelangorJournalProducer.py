# kafka
from kafka import KafkaProducer
import json
import time
import pickle
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service



class SelangorJournalProducer:
    def __init__(self, kafka_server='localhost:9092', topic='selangor_journal_topic'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic

    def send_article(self, article):
        self.producer.send(self.topic, article)
        print(f"Sent: {article['headline']}")

    def close(self):
        self.producer.flush()
        self.producer.close()


class SelangorJournalScraper:
    def __init__(self, base_url, pages=2):
        self.base_url = base_url
        self.pages = pages
        self.section_urls = self.generate_section_urls()
        self.news_urls = []
        self.data = []

    def generate_section_urls(self):
        urls = [self.base_url]
        for page in range(2, self.pages + 1):
            urls.append(f"{self.base_url}page/{page}/")
        return urls

    def _create_driver(self):
        user_agent = UserAgent()
        options = Options()
        options.add_argument("--headless")
        options.add_argument(f"user-agent={user_agent.random}")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def collect_article_urls(self):
        for section_url in self.section_urls:
            driver = self._create_driver()
            try:
                driver.get(section_url)
                time.sleep(2)
                url_elements = driver.find_elements(By.CSS_SELECTOR, '.penci-link-post.penci-image-holder.penci-disable-lazy')
                for element in url_elements:
                    href = element.get_attribute('href')
                    if href:
                        self.news_urls.append(href)
            except Exception as e:
                print(f"Error on section page {section_url}: {e}")
            finally:
                driver.quit()
        print(f"Collected {len(self.news_urls)} article URLs.")

    def scrape_and_stream_articles(self, producer):
        for url in self.news_urls:
            driver = self._create_driver()
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )

                headline = driver.find_element(By.TAG_NAME, "h1").text
                date = driver.find_element(By.TAG_NAME, "time").get_attribute('datetime')

                content = ''
                possible_classes = ["dable-content-wrapper", "entry-content", "article-content"]
                for cls in possible_classes:
                    if driver.find_elements(By.CLASS_NAME, cls):
                        wrapper = driver.find_element(By.CLASS_NAME, cls)
                        paragraphs = wrapper.find_elements(By.TAG_NAME, "p")
                        content = ' '.join([p.text for p in paragraphs])
                        break

                if content:
                    article_data = {
                        'url': url,
                        'headline': headline,
                        'date_published': date,
                        'article_content': content
                    }
                    producer.send_article(article_data)
                else:
                    print(f"No content found on {url}")

            except Exception as e:
                print(f"Error scraping {url}: {e}")
            finally:
                driver.quit()


if __name__ == "__main__":
    scraper = SelangorJournalScraper(
        base_url='https://selangorjournal.my/category/current/crime/',
        pages=2
    )

    producer = SelangorJournalProducer()

    scraper.collect_article_urls()
    scraper.scrape_and_stream_articles(producer)

    producer.close()


spark.stop()
