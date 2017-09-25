package com.zx.kafka.Consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {

	public static void main(String[] args) {

		Properties pros = new Properties();
		pros.put("bootstrap.servers", "47.95.15.125:9092");
		/* key的序列化类 */
		pros.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		/* value的序列化类 */
		pros.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		pros.put("enable.auto.commit", "true");
		/* 制定consumer group */
		pros.put("group.id", "test");

		Consumer<String, String> consumer = new KafkaConsumer<>(pros);
		consumer.subscribe(Arrays.asList("test"));

		while (true) {
			/* 读取数据，读取超时时间为100ms */
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
		}
	}

}
