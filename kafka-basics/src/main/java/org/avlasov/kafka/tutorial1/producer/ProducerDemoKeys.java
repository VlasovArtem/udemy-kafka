package org.avlasov.kafka.tutorial1.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemoKeys extends KafkaAbstract {

    public ProducerDemoKeys() {
        super("/kafka-producer.properties");
    }

    @Override
    public void perform(String topic) {
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            for (int i = 0; i < 10; i++) {
                String value = "hello world " + i;
                String key = "id_" + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                log.info("Key: {}", key);

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
                }).get(); //block the .send() to make it synchronous - don't do this in production!
            }

            kafkaProducer.flush();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args){
        new ProducerDemoKeys().perform("first_topic");
    }
}
