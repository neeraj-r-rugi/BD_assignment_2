import os
import time
from kafka import KafkaConsumer


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    topic_name = 'order_metrics'
    landing_dir = 'vm1/landing'
    landing_file = os.path.join(landing_dir, 'order_metrics_landed.jsonl')

    os.makedirs(landing_dir, exist_ok=True)

    print(f"Connecting to Kafka at {bootstrap_servers} to consume from '{topic_name}'...")

    # Fresh group per run + no offset commits: every run re-reads the topic from the
    # beginning, so the landing file is never silently overwritten with nothing.
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f"vm1-metrics-{int(time.time())}",
        value_deserializer=lambda x: x.decode('utf-8', errors='replace'),
        consumer_timeout_ms=15000,
    )

    print(f"Writing consumed metrics to landing file: {landing_file}")
    count = 0

    with open(landing_file, 'w', encoding='utf-8') as f:
        for message in consumer:
            f.write(message.value.replace('\n', ' ').replace('\r', ' ') + '\n')
            count += 1
            print(f"Landed metric record {count} (partition {message.partition}, offset {message.offset})")

    consumer.close()
    print(f"Successfully landed {count} records into {landing_file}.")


if __name__ == '__main__':
    main()