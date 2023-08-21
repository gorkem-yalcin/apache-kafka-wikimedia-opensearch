package com.gorkem.producer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {

	private KafkaProducer<String, String> producer;
	private String topic;

	private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class);

	public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	@Override
	public void onOpen() {
		logger.info("onOpen");
	}

	@Override
	public void onClosed() {
		logger.info("onClosed");

		producer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) {
		logger.info("onMessage: " + messageEvent.getData());

		producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
	}

	@Override
	public void onComment(String s) {
		logger.info("onComment");
	}

	@Override
	public void onError(Throwable throwable) {
		logger.info("onError" + throwable);
	}
}
