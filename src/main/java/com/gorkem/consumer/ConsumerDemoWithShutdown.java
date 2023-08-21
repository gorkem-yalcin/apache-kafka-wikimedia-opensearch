package com.gorkem.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

	public static void main(String[] args) {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='tourismland' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0b3VyaXNtbGFuZCIsIm9yZ2FuaXphdGlvbklkIjo3NTc0NSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyMmFlZGZiNi1lMTA3LTRjNGEtYTA0Zi0wNmU5YTBjZDNlYzQifX0._LNbajfc7AHdZYQDegx-zByU4VhlPTT72j8LOJ7D1A0';");
		properties.setProperty("sasl.mechanism", "PLAIN");

		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		String groupId = "my-java";
		properties.setProperty("group.id", groupId);
		properties.setProperty("auto.offset.reset", "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		final Thread mainThread = Thread.currentThread();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("shutdown detected!!!!!!");
			consumer.wakeup();

			try {
				mainThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}));

		try {
			consumer.subscribe(Arrays.asList("wikimedia"));

			while (true) {
				logger.info("polling");
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					logger.info(record.key() + " | " + record.value());
					logger.info(record.partition() + " | " + record.offset());
				}
			}

		} catch (WakeupException e) {
			logger.info("shutdown");
		} catch (Exception e) {
			logger.error("unexpected exception");
		} finally {
			consumer.close();
			logger.error("gracetown");
		}

	}
}