package com.gorkem.consumer;

import com.gorkem.streambuilder.BotCountStreamBuilder;
import com.gorkem.streambuilder.EventCountTimeseriesBuilder;
import com.gorkem.streambuilder.WebsiteCountStreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikimediaStreamsApp {
	private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaStreamsApp.class);
	private static final Properties properties;
	private static final String INPUT_TOPIC = "wikimedia";

	static {
		properties = new Properties();

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='tourismland' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0b3VyaXNtbGFuZCIsIm9yZ2FuaXphdGlvbklkIjo3NTc0NSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyMmFlZGZiNi1lMTA3LTRjNGEtYTA0Zi0wNmU5YTBjZDNlYzQifX0._LNbajfc7AHdZYQDegx-zByU4VhlPTT72j8LOJ7D1A0';");
		properties.setProperty("sasl.mechanism", "PLAIN");

		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application-2");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	}

	public static void main(String[] args) {
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> changeJsonStream = builder.stream(INPUT_TOPIC);

		BotCountStreamBuilder botCountStreamBuilder = new BotCountStreamBuilder(changeJsonStream);
		botCountStreamBuilder.setup();

		WebsiteCountStreamBuilder websiteCountStreamBuilder = new WebsiteCountStreamBuilder(changeJsonStream);
		websiteCountStreamBuilder.setup();

		EventCountTimeseriesBuilder eventCountTimeseriesBuilder = new EventCountTimeseriesBuilder(changeJsonStream);
		eventCountTimeseriesBuilder.setup();

		final Topology appTopology = builder.build();
		LOGGER.info("Topology: {}", appTopology.describe());
		KafkaStreams streams = new KafkaStreams(appTopology, properties);
		streams.start();
	}
}