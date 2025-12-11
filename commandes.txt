cd /usr/local/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
sudo systemctl start influxdb
ouvrir : http://localhost:8086/onboarding/0
influxdb: imen.123456789   /admin

API :NiWBZzKUEbLIPPC7QN_tJU8MyorcVtds8itF347ok49Ma_euClbNIcn0AZ0CDytAf9pqsQI0iyyF9dHesMUDfw==

userid: 0fef82057358e000
orgid: e0f07bd6734e5497
token_acces: 

source /home/imen/spark-env/bin/activate
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 process_spark_streaming.py