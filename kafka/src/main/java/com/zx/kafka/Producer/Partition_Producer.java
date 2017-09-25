package com.zx.kafka.Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Partition_Producer {

	private static Producer<String, String> producer;
	Properties props = new Properties();

	public Partition_Producer() {
		props.put("bootstrap.servers", "slave01:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.zx.kafka.CustomPartition");
		producer = new KafkaProducer<>(props);
	}

	public static void main(String[] args) {
		Partition_Producer sproducer = new Partition_Producer();

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test3", "ljs ,this is win7");
		producer.send(record);
		producer.close();
	}

}
