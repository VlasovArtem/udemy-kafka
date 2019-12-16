package org.avlasov.kafka.tutorial1.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class ConsumerDemoAssignSeek extends KafkaAbstract {

    public ConsumerDemoAssignSeek() {
        super("/kafka-consumer.properties");
    }

    @Override
    public void perform(String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            TopicPartition topicPartition = new TopicPartition(topic, 0);
            long offsetToReadFrom = 15L;
            consumer.assign(Collections.singletonList(topicPartition));

            consumer.seek(topicPartition, offsetToReadFrom);

            int numberOfMessagesToRead = 5;
            boolean keepOnReading = true;
            int numberOfMessagesReadSoFar = 0;

            while (keepOnReading) {
                ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    numberOfMessagesReadSoFar++;
                    log.info("Key: {}, Value: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
                    if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                        keepOnReading = false;
                        break;
                    }
                }
            }
        }
        log.info("Exiting the application");
    }

    public static void main(String[] args) {
        new ConsumerDemoAssignSeek().perform("first_topic");
    }

}
