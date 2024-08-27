1. start zookeeper

sh bin/zookeeper-server-start.sh config/zookeeper.properties 

2. start kafka server 

sh bin/kafka-server-start.sh config/server.properties 


3. create a topic 

sh bin/kafka-topics.sh --delete --topic test_producer01 --bootstrap-server localhost:9092


sh bin/kafka-topics.sh --create --topic test_producer02 --bootstrap-server localhost:9092


4. validate the topic 

sh bin/kafka-topics.sh --describe --topic test_producer01 --bootstrap-server localhost:9092

5. push messages into topic 

sh bin/kafka-console-producer.sh --topic test_producer02 --bootstrap-server localhost:9092 


Note: Make sure you only one send one message at a time 


{"name": "John Smith","sku" : "20223", "price"  : 23.95}
{"name": "A b","sku" : "20223", "price"  : 23.95}
{"name": "B c","sku" : "2022", "price"  : 23.95}
{"name": "D e","sku" : "2021", "price"  : 23.95}
{"name": "F g","sku" : "2022", "price"  : 23.95}
{"name": "H i","sku" : "223", "price"  : 23.95}


6. read messages pushed into topic 

sh bin/kafka-console-consumer.sh --topic test_producer01 --from-beginning --bootstrap-server localhost:9092 --group flink-test


{"name": "John Smith","sku" : "20223", "price"  : 23.95}
{"name": "A b","sku" : "20223", "price"  : 23.95}
{"name": "B c","sku" : "2022", "price"  : 23.95}
{"name": "D e","sku" : "2021", "price"  : 23.95}
{"name": "F g","sku" : "2022", "price"  : 23.95}
{"name": "H i","sku" : "223", "price"  : 23.95}

