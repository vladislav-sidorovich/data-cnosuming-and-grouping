package com.example.kafkaload.consumer;

import javax.annotation.PostConstruct;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

import com.example.kafkaload.Notification;
import com.example.kafkaload.container.NotificationsContainer;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.example.kafkaload.Constants.*;


@Import(KafkaConsumerConfig.class)
@ComponentScan(basePackages = "com.example.kafkaload.container")
@SpringBootApplication
public class KafkaReaderApplication implements CommandLineRunner, ConsumerSeekAware {
    private static final Logger logger = LoggerFactory.getLogger(KafkaReaderApplication.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    private final CountDownLatch latch = new CountDownLatch(3);

    public static void main(String[] args) {
        SpringApplication.run(KafkaReaderApplication.class, args);
    }

    @Autowired
    private NotificationsContainer container;

    private LongAdder counter = new LongAdder();
    private LongAdder bytes = new LongAdder();
    private Clock clock = Clock.systemDefaultZone();

    private long startTime;

    @PostConstruct
    public void init() {
        startTime = clock.instant().getEpochSecond();
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Reading Kafka data to container..");

        latch.await(10, TimeUnit.SECONDS);

        logger.info("Data read");

    }

    @KafkaListener(topics = "notifications")
    public void listen(ConsumerRecord<String, String> record) throws Exception {
        String value = record.value();
        counter.increment();
        bytes.add(value.getBytes().length);

        Notification notification = null;
        try {
            notification = objectMapper.readValue(value, Notification.class);
        } catch (Exception e) {
            logger.warn("Wrong message {} ", value);
        }
        container.add(notification);

        long now = System.currentTimeMillis();
        if ((now % 1_000) == 0) {
            long totalMessages = counter.longValue();
            long totalBytes = bytes.longValue();

            logger.info("Count of messages consumes [{}]. Data size [{}]. Notifications in memory [{}]",
                    totalMessages, humanReadableByteCountBin(totalBytes), container.size());

            long runningTime = Math.max(clock.instant().getEpochSecond() - startTime, 1);
            logger.info("Running [{}] seconds. Consuming speed [{}] per second", runningTime, totalMessages / runningTime);
            logger.info("The latest message offset [{}]", record.offset());
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().stream()
                .filter(partition -> kafkaNotificationsTopic.equals(partition.topic()))
                .forEach(partition -> callback.seekToBeginning(kafkaNotificationsTopic, partition.partition()));
    }
}
