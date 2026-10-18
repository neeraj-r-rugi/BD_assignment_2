import os
from kafka import KafkaProducer


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    topic_name = 'orders'
    input_file = 'data/input/order.jsonl'

    if not os.path.exists(input_file):
        print(f"Input file not found at {input_file}. Please ensure your dataset is placed there.")
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

    print(f"Reading orders from {input_file} and publishing to topic '{topic_name}'...")
    count = 0
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Publish the raw line as-is. Malformed / invalid records are
            # ignored later by Spark Job 1, as required by the assignment.
            producer.send(topic_name, value=line).add_errback(on_error)
            count += 1
            print(f"Published line {count}")

    producer.flush()
    producer.close()

    if errors:
        print(f"WARNING: {len(errors)} records failed to publish: {errors[0]}")
    print(f"Successfully published {count - len(errors)} of {count} records to Kafka topic '{topic_name}'.")


if __name__ == '__main__':
    main()