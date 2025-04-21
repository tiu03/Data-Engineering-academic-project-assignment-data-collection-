from kafka import KafkaConsumer
import json
import csv

class ArticleConsumer:
    def __init__(self, kafka_server='localhost:9092', topic='news_articles', group_id='news_group'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def consume_and_save(self, output_file='kafka_articles_output.csv'):
        print("Consuming messages...")
        fieldnames = ['url', 'headline', 'author', 'publish_time', 'article_content']

        with open(output_file, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for message in self.consumer:
                article = message.value
                writer.writerow(article)
                print(f"Saved: {article['headline'][:60]}")

# Example usage:
consumer = ArticleConsumer()
consumer.consume_and_save()
