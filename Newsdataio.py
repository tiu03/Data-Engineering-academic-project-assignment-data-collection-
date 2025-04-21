from pyspark.sql import SparkSession

import sys

sys.path.append(r'/home/student/data_collected')


from newsdataapi import NewsDataApiClient
import pickle
import csv  
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import time


spark = SparkSession.builder.appName('Newsdata io').getOrCreate()

class NewsFetcher:
    def __init__(self, api, country='us', category='crime', language='en'):
        self.api = api
        self.country = country
        self.category = category
        self.language = language
        self.news_list = []

    def fetch_all_news(self):
        page = None
        while True:
            try:
                response = self.api.news_api(
                    country=self.country,
                    category=self.category,
                    language=self.language,
                    page=page
                )
                news = response.get('results', [])
                self.news_list.extend(news)

                page = response.get('nextPage', None)
                if not page:
                    print("No more pages available.")
                    break
            except Exception as e:
                print(f"Error: {e}")
                break

    def get_news(self):
        return self.news_list


class NewsAnalyzer:
    def __init__(self, news_list):
        self.news_list = news_list

    def display_sample_news(self, count=5):
        for i, news in enumerate(self.news_list[:count]):
            print(f"News {i}: {news}")

    def get_all_keys(self):
        all_keys = set()
        for news in self.news_list:
            all_keys.update(news.keys())
        return all_keys


class NewsExporter:
    def __init__(self, news_list, all_keys, spark):
        self.news_list = news_list
        self.all_keys = list(all_keys)
        self.spark = spark   

    def export_to_csv(self, filename='newsdata_io_14_4_test.csv'):
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.all_keys)
                writer.writeheader()
                for news in self.news_list:
                    writer.writerow(news)
            print(f"Exported {len(self.news_list)} articles to {filename}")
        except Exception as e:
            print(f"Failed to write CSV: {e}")
            



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












class ArticleConsumer:
    def __init__(self, kafka_server='localhost:9092', topic='news_io_topic'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset='earliest',
            # auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
# # Subscribe to a specific topic
# consumer.subscribe(topics=['my-topic'])
    
    def consume_and_save(self, output_file='kafka_new_io_articles_output.csv'):
        print("Consuming messages...")
        fieldnames = ['title', 'content', 'pubDate', 'link','category']  

        with open(output_file, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for message in self.consumer:
                article = message.value
                writer.writerow(article)
                print(f"Saved: {article['title'][:60]}") 
                # Add to in-memory list for HDFS write
                self.articles.append(article)
                
                if len(self.articles) >= 100:
                    self.export_to_hdfs(hdfs_path)
                    self.articles = []  # reset batch
                
    def export_to_hdfs(self, hdfs_path):
        if not hdfs_path:
            print("No HDFS path provided, skipping export.")
            return

        df = self.spark.createDataFrame(self.articles)
        df.coalesce(1).write.mode('append').csv(hdfs_path, header=True)
        print(f"Exported {len(self.articles)} articles to HDFS at {hdfs_path}")

api = NewsDataApiClient(apikey="pub_74815e364b4e931611590c50e5c72985ca80a")
fetcher = NewsFetcher(api)
fetcher.fetch_all_news()

news_list = fetcher.get_news()
print(f"Total articles retrieved: {len(news_list)}")

analyzer = NewsAnalyzer(news_list)
analyzer.display_sample_news()
all_keys = analyzer.get_all_keys()
print("All keys found in articles:", all_keys)

exporter = NewsExporter(news_list, all_keys, spark)
exporter.export_to_csv()



# if __name__ == "__main__":
    
#     kafka_servers = 'localhost:9092'
#     kafka_topic = 'news_io_topic'
#     news_api_key = 'pub_74815e364b4e931611590c50e5c72985ca80a'

#     producer = NewsKafkaProducer(
#         kafka_bootstrap_servers=kafka_servers,
#         kafka_topic=kafka_topic,
#         news_api_key=news_api_key
#     )

#     try:
#         producer.fetch_and_publish_news(max_messages=30)
#     finally:
#         producer.close()



# # Example usage:
# consumer = ArticleConsumer()

# consumer.consume_and_save(output_file='kafka_new_io_articles_output.csv', hdfs_path=''hdfs://localhost:9000/user/student/data_collected/newsdata_io'')
spark.stop()

