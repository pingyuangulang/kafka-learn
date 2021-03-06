package com.five.monkey.kafka.learn.chapter1;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 自定义拦截器
 * @author jim
 * @date 2021/1/30 16:18
 */
public class MyInterceptor implements ProducerInterceptor<String, String> {

	/**
	 * This is called from {@link KafkaProducer#send(ProducerRecord)} and
	 * {@link KafkaProducer#send(ProducerRecord, Callback)} methods, before key and value
	 * get serialized and partition is assigned (if partition is not specified in ProducerRecord).
	 * <p>
	 * This method is allowed to modify the record, in which case, the new record will be returned. The implication of modifying
	 * key/value is that partition assignment (if not specified in ProducerRecord) will be done based on modified key/value,
	 * not key/value from the client. Consequently, key and value transformation done in onSend() needs to be consistent:
	 * same key and value should mutate to the same (modified) key and value. Otherwise, log compaction would not work
	 * as expected.
	 * <p>
	 * Similarly, it is up to interceptor implementation to ensure that correct topic/partition is returned in ProducerRecord.
	 * Most often, it should be the same topic/partition from 'record'.
	 * <p>
	 * Any exception thrown by this method will be caught by the caller and logged, but not propagated further.
	 * <p>
	 * Since the producer may run multiple interceptors, a particular interceptor's onSend() callback will be called in the order
	 * specified by {@link ProducerConfig#INTERCEPTOR_CLASSES_CONFIG}. The first interceptor
	 * in the list gets the record passed from the client, the following interceptor will be passed the record returned by the
	 * previous interceptor, and so on. Since interceptors are allowed to modify records, interceptors may potentially get
	 * the record already modified by other interceptors. However, building a pipeline of mutable interceptors that depend on the output
	 * of the previous interceptor is discouraged, because of potential side-effects caused by interceptors potentially failing to
	 * modify the record and throwing an exception. If one of the interceptors in the list throws an exception from onSend(), the exception
	 * is caught, logged, and the next interceptor is called with the record returned by the last successful interceptor in the list,
	 * or otherwise the client.
	 *
	 * @param record the record from client or the record returned by the previous interceptor in the chain of interceptors.
	 * @return producer record to send to topic/partition
	 */
	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		String newValue = "hello jim, " + record.value() + ", bye jim";
		System.out.println("interceptor拦截器onSend=========================topic=" + record.topic() + ",partition=" + record.partition()
				+ "=========================");
		return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), newValue, record.headers());
	}

	/**
	 * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
	 * it gets sent to the server.
	 * <p>
	 * This method is generally called just before the user callback is called, and in additional cases when <code>KafkaProducer.send()</code>
	 * throws an exception.
	 * <p>
	 * Any exception thrown by this method will be ignored by the caller.
	 * <p>
	 * This method will generally execute in the background I/O thread, so the implementation should be reasonably fast.
	 * Otherwise, sending of messages from other threads could be delayed.
	 *
	 * @param metadata  The metadata for the record that was sent (i.e. the partition and offset).
	 *                  If an error occurred, metadata will contain only valid topic and maybe
	 *                  partition. If partition is not given in ProducerRecord and an error occurs
	 *                  before partition gets assigned, then partition will be set to RecordMetadata.NO_PARTITION.
	 *                  The metadata may be null if the client passed null record to
	 *                  {@link KafkaProducer#send(ProducerRecord)}.
	 * @param exception The exception thrown during processing of this record. Null if no error occurred.
	 */
	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		System.out.println("interceptor拦截器onAcknowledgement=========================topic=" + metadata.topic() + ",partition="
				+ metadata.partition() + ",offset=" + metadata.offset() + "=========================");
		if (Objects.nonNull(exception)) {
			exception.printStackTrace();
		}
	}

	/**
	 * This is called when interceptor is closed
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
}
