package com.gorkem.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

	private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

	public static void main(String[] args) throws InterruptedException {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='tourismland' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0b3VyaXNtbGFuZCIsIm9yZ2FuaXphdGlvbklkIjo3NTc0NSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyMmFlZGZiNi1lMTA3LTRjNGEtYTA0Zi0wNmU5YTBjZDNlYzQifX0._LNbajfc7AHdZYQDegx-zByU4VhlPTT72j8LOJ7D1A0';");
		properties.setProperty("sasl.mechanism", "PLAIN");

		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		String topic = "wikimedia";
		String url = "https://stream.wikimedia.org/v2/stream/recentchange";
		BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

		EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));

		BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);

		BackgroundEventSource eventSource = builder.build();

		eventSource.start();

		TimeUnit.MINUTES.sleep(10);

//		producer.flush();
//
//		producer.close();

		logger.info("Hello world from callback!");
	}
}