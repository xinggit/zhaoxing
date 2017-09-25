package com.zx.kafka.Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

	private static Producer<String, String> producer;
	Properties props = new Properties();

	public SimpleProducer() {
		props.put("bootstrap.servers", "47.95.15.125:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	public static void main(String[] args) {
		SimpleProducer sproducer = new SimpleProducer();

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "hello world ,i am win7");
		producer.send(record);
		producer.close();
	}

}
