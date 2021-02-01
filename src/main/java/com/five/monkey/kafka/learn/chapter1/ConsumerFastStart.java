package com.five.monkey.kafka.learn.chapter1;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author jim
 * @date 2021/1/29 20:56
 */
public class ConsumerFastStart {

	private static final String			brokerList	= "47.94.18.6:9092";

	private static final List<String>	topics		= Stream.of("vivo", "jim").collect(Collectors.toList());

	private static final String			groupId		= "group.vivo";

	public static void main(String[] args) {
		Properties properties = new Properties();
		// 设置key反序列化器
		// properties.put("key.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// 设置value反序列化器
		// properties.put("value.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// 设置集群地址
		// properties.put("bootstrap.servers", brokerList);
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		// 消费者组
		// properties.put("group.id", groupId);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(topics);
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));
			for (ConsumerRecord<String, String> record : records) {
				StringBuilder msg = new StringBuilder();
				msg.append("topic=").append(record.topic()).append(",offset=").append(record.offset()).append(",partition=")
						.append(record.partition()).append(",value=").append(record.value());
				FileUtil.write(msg.toString());
			}
		}
	}
}
