from kafka import KafkaConsumer
class SelangorJournalConsumer:
    def __init__(self, kafka_server='localhost:9092', topic='selangor_journal_topic'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def consume_and_save(self, output_file='selangor_journal_output.csv'):
        print("Consuming messages...")
        fieldnames = ['url', 'headline', 'date_published', 'article_content']

        with open(output_file, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for message in self.consumer:
                article = message.value
                writer.writerow(article)
                print(f"Saved: {article['headline'][:60]}")

# Example:
consumer = SelangorJournalConsumer()
consumer.consume_and_save()

spark.stop()

