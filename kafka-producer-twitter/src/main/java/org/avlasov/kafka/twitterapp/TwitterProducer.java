package org.avlasov.kafka.twitterapp;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class TwitterProducer {

    private static final String TWITTER_OAUTH_CONSUMER_KEY_PROPERTY = "twitter.oauth.consumer.key";
    private static final String TWITTER_OAUTH_CONSUMER_SECRET_PROPERTY = "twitter.oauth.consumer.secret";
    private static final String TWITTER_OAUTH_TOKEN_PROPERTY = "twitter.oauth.token";
    private static final String TWITTER_OAUTH_TOKEN_SECRET_PROPERTY = "twitter.oauth.token.secret";
    private static final String TWITTER_SEARCH_TERMS = "twitter.search.terms";
    private static final String KAFKA_TOPIC = "kafka.topic";
    private final Properties properties;

    public TwitterProducer(Properties properties) {
        this.properties = properties;
    }

    public void run()  {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("shutting down client from twitter...");
            twitterClient.stop();
            log.info("closing producer...");
            kafkaProducer.close();
            log.info("done!");
        }));

        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if (msg != null) {
                kafkaProducer.send(new ProducerRecord<>(properties.getProperty(KAFKA_TOPIC), msg), getKafkaProducerCallback());
            }
        }
        log.info("End of application");
    }

    private Callback getKafkaProducerCallback() {
        return (metadata, exception) -> {
            if (exception != null) {
                log.error("Something bad happened", exception);
            }
        };
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList(properties.getProperty(TWITTER_SEARCH_TERMS).split(","))
                .stream()
                .map(String::trim)
                .collect(Collectors.toList());
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(properties.getProperty(TWITTER_OAUTH_CONSUMER_KEY_PROPERTY),
                properties.getProperty(TWITTER_OAUTH_CONSUMER_SECRET_PROPERTY),
                properties.getProperty(TWITTER_OAUTH_TOKEN_PROPERTY),
                properties.getProperty(TWITTER_OAUTH_TOKEN_SECRET_PROPERTY));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream(new File(args[0]));
        properties.load(fileInputStream);
        new TwitterProducer(properties).run();
    }

}
