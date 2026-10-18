import os
import time
from kafka import KafkaConsumer


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    topic_name = 'orders'
    landing_dir = 'vm4/landing'
    landing_file = os.path.join(landing_dir, 'orders_landed.jsonl')

    os.makedirs(landing_dir, exist_ok=True)

    print(f"Connecting to Kafka at {bootstrap_servers} to consume from '{topic_name}'...")

    # A fresh consumer group per run + no offset commits means every run reads the
    # topic from the beginning. (A fixed group with auto-commit would return 0
    # messages on a re-run and leave an empty landing file.)
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f"vm4-landing-{int(time.time())}",
        value_deserializer=lambda x: x.decode('utf-8', errors='replace'),
        consumer_timeout_ms=15000,  # stop after 15 s with no new messages
    )

    print(f"Writing consumed orders to landing file: {landing_file}")
    count = 0

    with open(landing_file, 'w', encoding='utf-8') as f:
        for message in consumer:
            # One record per line, written exactly as received
            f.write(message.value.replace('\n', ' ').replace('\r', ' ') + '\n')
            count += 1
            print(f"Landed record {count} (partition {message.partition}, offset {message.offset})")

    consumer.close()
    print(f"Successfully landed {count} records into {landing_file}.")


if __name__ == '__main__':
    main()