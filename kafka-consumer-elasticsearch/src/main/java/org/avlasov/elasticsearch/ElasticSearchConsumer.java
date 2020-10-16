package org.avlasov.elasticsearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {

    private static final String ELASTICSEARCH_AUTH_USERNAME = "elasticsearch.auth.username";
    private static final String ELASTICSEARCH_AUTH_PASSWORD = "elasticsearch.auth.password";
    private static final String ELASTICSEARCH_HOST_NAME = "elasticsearch.host.name";
    private static final String ELASTICSEARCH_HOST_PORT = "elasticsearch.host.port";
    private static final String ELASTICSEARCH_HOST_SCHEME = "elasticsearch.host.scheme";
    private static final String ELASTICSEARCH_INDEX_NAME = "elasticsearch.index.name";
    private static final String ELASTICSEARCH_INDEX_CONTENT_TYPE = "elasticsearch.index.content.type";
    private static final String KAFKA_TOPIC = "kafka.topic";
    private final Properties properties;

    public ElasticSearchConsumer(Properties properties) {
        this.properties = properties;
    }

    public void run() throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();
            log.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {

                String tweetJson = record.value();
//                Kafka generic ID
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    String id = extractIdFromTweet(tweetJson);

                    IndexRequest indexRequest = new IndexRequest(properties.getProperty(ELASTICSEARCH_INDEX_NAME))
                            .id(id) //idempotent
                            .source(tweetJson, XContentType.fromMediaTypeOrFormat(properties.getProperty(ELASTICSEARCH_INDEX_CONTENT_TYPE)));

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    log.warn("skipping bad data: " + record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                log.info("Committing offsets...");
                consumer.commitSync();
                log.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private KafkaConsumer<String, String> createConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(properties.getProperty(KAFKA_TOPIC)));
        return consumer;
    }

    private RestHighLevelClient createClient() {
        //not required if you are using local ES
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(properties.getProperty(ELASTICSEARCH_AUTH_USERNAME), properties.getProperty(ELASTICSEARCH_AUTH_PASSWORD)));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(properties.getProperty(ELASTICSEARCH_HOST_NAME),
                        Integer.parseInt(properties.getProperty(ELASTICSEARCH_HOST_PORT)),
                        properties.getProperty(ELASTICSEARCH_HOST_SCHEME)))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(args[0])));
        new ElasticSearchConsumer(properties).run();
    }

}
