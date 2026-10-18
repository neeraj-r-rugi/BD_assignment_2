import os
from kafka import KafkaProducer


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    topic_name = 'order_metrics'
    metrics_file = 'vm4/output/order_metrics.jsonl'

    if not os.path.exists(metrics_file):
        print(f"Metrics file not found at {metrics_file}. Please run Spark Job 1 first.")
        return

    print(f"Connecting to Kafka at {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all',          # wait for all in-sync replicas (replication factor 2)
        retries=5,
        value_serializer=lambda line: line.encode('utf-8'),
    )

    errors = []

    def on_error(exc):
        errors.append(exc)

    print(f"Reading metrics from {metrics_file} and publishing to topic '{topic_name}'...")
    count = 0
    with open(metrics_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            producer.send(topic_name, value=line).add_errback(on_error)
            count += 1
            print(f"Published metric record {count}")

    producer.flush()
    producer.close()

    if errors:
        print(f"WARNING: {len(errors)} records failed to publish: {errors[0]}")
    print(f"Successfully published {count - len(errors)} of {count} records to Kafka topic '{topic_name}'.")


if __name__ == '__main__':
    main()