package com.five.monkey.kafka.learn.chapter1;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * 自定义分区器
 * @author jim
 * @date 2021/1/30 15:27
 */
public class MyPartitioner implements Partitioner {

	private static final Map<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

	/**
	 * 获取partition编号
	 *
	 * @param topic      The topic name
	 * @param key        The key to partition on (or null if no key)
	 * @param keyBytes   The serialized key to partition on( or null if no key)
	 * @param value      The value to partition on or null
	 * @param valueBytes The serialized value to partition on or null
	 * @param cluster    The current cluster metadata
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
		int availableCounter = partitionInfoList.size();
		int index;
		// 如果key是空的
		if (Objects.isNull(keyBytes) || keyBytes.length == 0) {
			int nextValue = this.nextValue(topic);
			index = nextValue % availableCounter;
		} else {
			index = Utils.toPositive(Utils.murmur2(keyBytes)) % availableCounter;
		}
		int partitionNumber = partitionInfoList.get(index).partition();
		System.out.println("partitioner分区器=========================partitionNumber=" + partitionNumber + "=========================");
		return partitionNumber;
	}

	/**
	 * This is called when partitioner is closed.
	 */
	@Override
	public void close() {

	}

	/**
	 * Configure this class with the given key-value pairs
	 *
	 * @param configs
	 */
	@Override
	public void configure(Map<String, ?> configs) {

	}

	/**
	 * 当没有指定key时,对partition进行轮训
	 * @param topic The topic name
	 * @return next number
	 */
	private int nextValue(String topic) {
		AtomicInteger counter = topicCounterMap.get(topic);
		if (Objects.isNull(counter)) {
			counter = new AtomicInteger(0);
			AtomicInteger value = topicCounterMap.putIfAbsent(topic, counter);
			if (Objects.nonNull(value)) {
				counter = value;
			}
		}
		return counter.getAndUpdate((operand) -> operand + 1);
	}
}
