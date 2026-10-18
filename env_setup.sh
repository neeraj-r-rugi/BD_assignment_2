sudo docker compose down -v
sleep 10
sudo docker compose up -d
sleep 10
# then run the two --create commands above

sudo docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --topic orders --partitions 2 --replication-factor 2
sudo docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --topic order_metrics --partitions 2 --replication-factor 2

echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=/usr/bin/python3.10' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10' >> ~/.bashrc
source ~/.bashrc

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
export PATH=$JAVA_HOME/bin:$PATH

source .venv/bin/activate

python3.10 src/producer/producer.py && \
python3.10 src/consumer/consumer.py && \
python3.10 src/spark/spark_job_1.py && \
python3.10 src/producer/metrics_producer.py && \
python3.10 src/consumer/metrics_consumer.py && \
python3.10 src/spark/spark_job_2_summaries.py

sleep 5
sudo docker exec -it kafka-1 kafka-run-class kafka.tools.GetOffsetShell   --bootstrap-server localhost:29092   --topic orders