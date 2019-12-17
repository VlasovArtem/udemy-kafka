package org.avlasov.kafka.tutorial1.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

@Slf4j
public class ProducerDemoWithCallback extends KafkaAbstract {

    public ProducerDemoWithCallback() {
        super("/kafka-producer.properties");
    }

    @Override
    public void perform(String topic) {
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello world " + i);

                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error callback", exception);
                    }
                });
            }

            kafkaProducer.flush();
        }
    }

    public static void main(String[] args) {
        new ProducerDemoWithCallback().perform("first_topic");
    }
}
