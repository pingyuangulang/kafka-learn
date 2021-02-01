package com.five.monkey.kafka.learn.chapter1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jim
 * @date 2021/1/29 20:29
 */
public class ProducerFastStart {

	private static final String	brokerList	= "47.94.18.6:9092";

	private static final String	topic		= "vivo";

	public static void main(String[] args) {
		Properties properties = new Properties();
		// 设置key序列化器
		// properties.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// 设置value序列化器
		// properties.put("value.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// 设置重试次数
		properties.put(ProducerConfig.RETRIES_CONFIG, 10);
		// 设置集群地址
		// properties.put("bootstrap.servers", brokerList);
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		// 设置partition分区器
		properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
		// 设置拦截器
		properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyInterceptor.class.getName());
		/*
		 * 生产者确定消息送达到服务器的响应模式
		 * 0:生产者不需要等待来自服务器的响应，生产者只管写入，不管写入成功与否。优点:吞吐量高;缺点:容易数据丢失
		 * 1:默认值，生产者只需等待leader节点的响应即可。防数据丢失和吞吐量的权衡之策。
		 * 当leader写入成功后还没来得及同步给follower时leader挂了，此时收据丢失
		 * -1:leader和follower都收到消息才向生产者响应写入成功。优点:数据不易丢失;缺点:吞吐量低
		 */
		properties.put(ProducerConfig.ACKS_CONFIG, "-1");

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "I live in china, I'm a chinese");
		ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "I live in china, I'm a chinese");
		try {
			producer.send(record1);
			producer.send(record2);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
