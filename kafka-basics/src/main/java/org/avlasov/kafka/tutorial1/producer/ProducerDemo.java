package org.avlasov.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

public class ProducerDemo extends KafkaAbstract {

    public ProducerDemo() {
        super("/kafka-producer.properties");
    }

    @Override
    public void perform(String topic) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello world");

        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public static void main(String[] args) {
       new ProducerDemo().perform("first_topic");
    }

}
