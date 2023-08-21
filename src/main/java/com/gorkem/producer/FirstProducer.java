package com.gorkem.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FirstProducer {

	private static final Logger logger = LoggerFactory.getLogger(FirstProducer.class);

	public static void main(String[] args) {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='tourismland' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0b3VyaXNtbGFuZCIsIm9yZ2FuaXphdGlvbklkIjo3NTc0NSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyMmFlZGZiNi1lMTA3LTRjNGEtYTA0Zi0wNmU5YTBjZDNlYzQifX0._LNbajfc7AHdZYQDegx-zByU4VhlPTT72j8LOJ7D1A0';");
		properties.setProperty("sasl.mechanism", "PLAIN");

		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello space");

		producer.send(producerRecord);

		producer.flush();

		producer.close();

		logger.info("Hello world!");
	}
}