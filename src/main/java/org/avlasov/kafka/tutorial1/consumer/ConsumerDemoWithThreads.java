package org.avlasov.kafka.tutorial1.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.avlasov.kafka.tutorial1.KafkaAbstract;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThreads extends KafkaAbstract {

    public ConsumerDemoWithThreads() {
        super("/kafka-consumer.properties");
    }

    @Override
    public void perform(String topic) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(countDownLatch, topic);
        Thread thread = new Thread(consumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerThread.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().perform("first_topic");
    }

    private class ConsumerThread implements Runnable {

        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        private ConsumerThread(CountDownLatch latch, String topic) {
            this.latch = latch;
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        log.info("Key: {}, Value: {}", consumerRecord.key(), consumerRecord.value());
                        log.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

    }

}
