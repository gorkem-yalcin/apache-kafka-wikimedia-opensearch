package com.gorkem.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumerDemoWithShutdown {

	private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumerDemoWithShutdown.class);

	public static RestHighLevelClient createOpenSearchClient() {
		String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

		// we build a URI from the connection string
		RestHighLevelClient restHighLevelClient;
		URI connUri = URI.create(connString);
		// extract login information if it exists
		String userInfo = connUri.getUserInfo();

		if (userInfo == null) {
			// REST client without security
			restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

		} else {
			// REST client with security
			String[] auth = userInfo.split(":");

			CredentialsProvider cp = new BasicCredentialsProvider();
			cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
							.setHttpClientConfigCallback(
									httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
											.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


		}

		return restHighLevelClient;
	}

	public static void main(String[] args) throws IOException {

		try (RestHighLevelClient restHighLevelClient = createOpenSearchClient(); KafkaConsumer<String, String> consumer = createKafKaConsumer()) {
			boolean indexExists = restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
			if (!indexExists) {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				logger.info("Index created");
			} else {
				logger.info("Index already exists");
			}

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
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
					int recordCount = records.count();
					logger.info("Received " + recordCount + " record(s)");

					BulkRequest bulkRequest = new BulkRequest();

					for (ConsumerRecord<String, String> record : records) {

						String id = extractId(record.value());

						IndexRequest indexRequest = new IndexRequest("wikimedia")
								.source(record.value(), XContentType.JSON).id(id);
						bulkRequest.add(indexRequest);


					}
					if (bulkRequest.numberOfActions() > 0) {
						BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
						logger.info(bulkResponse.getItems().length + " | Inserted in bulk into OpenSearch");
						Thread.sleep(3000);
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

	private static String extractId(String json) {
		return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
	}

	private static KafkaConsumer<String, String> createKafKaConsumer() {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='tourismland' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0b3VyaXNtbGFuZCIsIm9yZ2FuaXphdGlvbklkIjo3NTc0NSwidXNlcklkIjpudWxsLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyMmFlZGZiNi1lMTA3LTRjNGEtYTA0Zi0wNmU5YTBjZDNlYzQifX0._LNbajfc7AHdZYQDegx-zByU4VhlPTT72j8LOJ7D1A0';");
		properties.setProperty("sasl.mechanism", "PLAIN");

		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		String groupId = "my-java";
		properties.setProperty("group.id", groupId);
		properties.setProperty("auto.offset.reset", "latest");

		return new KafkaConsumer<>(properties);
	}
}