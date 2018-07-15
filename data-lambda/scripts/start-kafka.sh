cd kafka_2.11-1.1.0
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server-1.properties &
bin/kafka-server-start.sh config/server-2.properties &


cd kafka_2.11-1.1.0
bin/kafka-server-start.sh config/server-3.properties &
bin/kafka-server-start.sh config/server-4.properties &