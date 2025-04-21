from pyspark.sql import SparkSession

import sys

sys.path.append(r'/home/student/data_collected')


from newsdataapi import NewsDataApiClient
import pickle
import csv  

spark = SparkSession.builder.appName('Newsdata_io_producer').getOrCreate()

from kafka import KafkaProducer
from newsdataapi import NewsDataApiClient
import json
import time


class NewsKafkaProducer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, news_api_key):
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Initialize NewsData API client
        self.api_client = NewsDataApiClient(apikey=news_api_key)

        # Kafka topic
        self.topic = kafka_topic

    def fetch_and_publish_news(self, category='crime', language='en', max_messages=None):
        page = None
        message_count = 0

        while True:
            response = self.api_client.news_api(
                category=category,
                language=language,
                page=page
            )

            news_list = response.get('results', [])

            if not news_list:
                print("No news found. Waiting for updates...")
                time.sleep(60)
                continue

            for news in news_list:
                news_data = {
                    "title": news.get("title"),
                    "content": news.get("description"),
                    "pubDate": news.get("pubDate"),
                    "link": news.get("link"),
                    "category": news.get("category")
                }

                self.send_to_kafka(news_data)
                message_count += 1

                if max_messages and message_count >= max_messages:
                    print(f"Reached limit of {max_messages} messages. Stopping.")
                    self.producer.flush()
                    return

            page = response.get('nextPage', None)

            if not page:
                print("No more pages. Waiting for new updates...")
                time.sleep(60)
            else:
                time.sleep(2)

    def send_to_kafka(self, message):
        """Send message to Kafka topic."""
        self.producer.send(self.topic, message)
        print(f"Sent: {message.get('title')}")

    def close(self):
        """Flush and close the Kafka producer."""
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    kafka_servers = 'localhost:9092'
    kafka_topic = 'news_io_topic'
    news_api_key = 'pub_74815e364b4e931611590c50e5c72985ca80a'

    producer = NewsKafkaProducer(
        kafka_bootstrap_servers=kafka_servers,
        kafka_topic=kafka_topic,
        news_api_key=news_api_key
    )

    try:
        producer.fetch_and_publish_news(max_messages=12)
    finally:
        producer.close()
        
spark.stop()