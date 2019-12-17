package org.avlasov.kafka.tutorial1.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class ConsumerDemo extends KafkaAbstract {

    public ConsumerDemo() {
        super("/kafka-consumer.properties");
    }

    @Override
    public void perform(String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key: {}, Value: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
                }
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerDemo().perform("first_topic");
    }

}
