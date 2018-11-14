package com.ssg.bigdata.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaDispatcher {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDispatcher.class);
	private static final String TOPIC = "APP_ERROR";

	public void dispatch(String data) {
		// TODO Auto-generated method stub

		Properties configs = new Properties();
		configs.put("bootstrap.servers", "10.203.5.86:9092,10.203.5.87:9092");
		configs.put("acks", "0");	//acks=0 If set to zero then the producer will not wait for any acknowledgment from the server at all. 
		configs.put("block.on.buffer.full", "true");
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

		producer.send(new ProducerRecord<>(TOPIC, data), (metadata, exception) -> {
			if (metadata != null) {
				System.out.println("partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");
				LOGGER.info("topic = {}, partition = {}, offset = {}, data = {}", metadata.topic(),metadata.partition(), metadata.offset(), data);
			} else {
				exception.printStackTrace();
			}
		});
		producer.flush();
		producer.close();

	}

}
