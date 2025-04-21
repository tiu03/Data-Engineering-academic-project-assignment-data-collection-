from pyspark.sql import SparkSession
import os
import sys

sys.path.append(r'/home/student/data_collected')

import time
import pickle
import csv
spark = SparkSession.builder.appName('Free Malaysia Today').getOrCreate()


import csv
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


class ArticleScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.driver = self._init_driver()
        self.article_urls = []
        self.articles_data = []

    def _init_driver(self):
        options = Options()
        options.add_argument("--headless")
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def scroll_and_load_articles(self):
        self.driver.get(self.base_url)
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            try:
                view_more = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.inline-flex.items-center"))
                )
                view_more.click()
                time.sleep(2)
            except Exception:
                break

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def collect_article_urls(self):
        articles = self.driver.find_elements(By.CSS_SELECTOR, 'section > article > a')
        self.article_urls = [a.get_attribute('href') for a in articles if a.get_attribute('href')]
        print(f"Collected {len(self.article_urls)} article URLs.")

    def extract_article_data(self, url):
        self.driver.get(url)
        wait = WebDriverWait(self.driver, 5)
        data = {'url': url}

        try:
            data['headline'] = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'h1'))).text
        except NoSuchElementException:
            data['headline'] = None

        try:
            data['author'] = self.driver.find_element(By.CSS_SELECTOR, '[class*="author"]').text
        except NoSuchElementException:
            data['author'] = None

        try:
            data['publish_time'] = self.driver.find_element(By.CSS_SELECTOR, '[property="article:published_time"]').get_attribute('content')
        except NoSuchElementException:
            data['publish_time'] = None

        try:
            paragraphs = self.driver.find_elements(By.CSS_SELECTOR, 'article p')
            data['article_content'] = '\n'.join([p.text for p in paragraphs if p.text])
        except NoSuchElementException:
            data['article_content'] = None

        return data

    def scrape_all_articles(self):
        for url in self.article_urls:
            try:
                article_data = self.extract_article_data(url)
                self.articles_data.append(article_data)
            except Exception as e:
                print(f"Error scraping {url}: {e}")
        print(f"Scraped {len(self.articles_data)} articles.")

    def export_to_csv(self, filename):
        try:
            with open(filename, 'w', encoding='utf-8-sig', newline='') as csvfile:
                fieldnames = ['url', 'headline', 'author', 'publish_time', 'article_content']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for article in self.articles_data:
                    writer.writerow(article)
            print(f"Exported to {filename}")
        except Exception as e:
            print(f"Failed to export CSV: {e}")

    def quit(self):
        self.driver.quit()


# # === Usage ===
if __name__ == "__main__":
    scraper = ArticleScraper("https://www.freemalaysiatoday.com/category/tag/police/")

    scraper.scroll_and_load_articles()
    scraper.collect_article_urls()
    scraper.scrape_all_articles()
    scraper.export_to_csv("freemalaysiatoday_police_articles1.csv")
    scraper.quit()


spark.stop()






















spark.stop()