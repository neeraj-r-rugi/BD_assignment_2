sudo apt update
sudo apt install openjdk-11-jdk

cd /opt
sudo wget https://mirror.ati.tn/apache/kafka/3.9.2/kafka_2.12-3.9.2.tgz
sudo tar -xvzf kafka_2.12-3.9.2.tgz
sudo mv kafka_2.12-3.9.2 kafka
sudo chown -R $USER:$USER /opt/kafka
sudo rm kafka_2.12-3.9.2.tgz

cat <<'EOF' >> ~/.bashrc

# Apache Kafka
export KAFKA_HOME=/opt/kafka
export PATH="$KAFKA_HOME/bin:$PATH"
EOF

source ~/.bashrc

echo "$KAFKA_HOME"
which kafka-topics.sh
kafka-topics.sh --version

mkdir -p /opt/kafka/data/kraft

sed -i 's|^log.dirs=.*|log.dirs=/opt/kafka/data/kraft|' $KAFKA_HOME/config/kraft/server.properties
grep -E '^(process.roles|node.id|listeners|log.dirs|controller.quorum.voters)=' $KAFKA_HOME/config/kraft/server.properties

KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
echo "$KAFKA_CLUSTER_ID"
kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c $KAFKA_HOME/config/kraft/server.properties

kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties
jps
ss -tlnp | grep -E '9092|9093'

kafka-server-stop.sh