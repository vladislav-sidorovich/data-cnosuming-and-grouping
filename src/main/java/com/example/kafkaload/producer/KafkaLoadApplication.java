package com.example.kafkaload.producer;

import javax.annotation.PostConstruct;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;

import com.example.kafkaload.Constants;
import com.example.kafkaload.Notification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.example.kafkaload.Constants.*;

@Import(KafkaProducerConfig.class)
@ComponentScan(basePackages = "com.example.kafkaload.container")
@SpringBootApplication
public class KafkaLoadApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLoadApplication.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("classpath:data/data.txt")
    private Resource resourceFile;


    public static void main(String[] args) {
        SpringApplication.run(KafkaLoadApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Loading data into Kafka...");

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            int game = i % games.size();
            int cluster = i % clusters.size();
            int data = i % this.data.size();

            Notification notification = Notification.builder()
                    .cluster(clusters.get(cluster))
                    .game(games.get(game))
                    .time(new Date())
                    .data(this.data.get(data))
                    .build();

            sendMessage(notification);
        }


        logger.info("Finishing");
    }

    private long totalBytes = 0;
    private long totalMessages = 0;

    private void sendMessage(Notification message) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(message);

        totalBytes += json.length() * 2;
        totalMessages++;

        kafkaTemplate.send(Constants.kafkaNotificationsTopic, json);

        if (totalMessages % 1000 == 0) {
            logger.info("Total messages [{}] and bytes [{}] send", totalMessages, humanReadableByteCountBin(totalBytes));
        }

    }

    private static final List<String> games;
    private static final List<String> clusters;
    private static final List<String> data;
    static {
        games = new ArrayList<>(20_000);
        for (int i = 0; i < 20_000; i++) {
            games.add("game-" + i);
        }

        clusters = new ArrayList<>(1_000);
        for (int i = 0; i < 1_000; i++) {
            clusters.add("c-" + i);
        }
        data = new ArrayList<>();
    }

    @PostConstruct
    public void initData() throws Exception {
        File file = resourceFile.getFile();
        Path path = Path.of(file.getPath());
        List<String> lines = Files.readAllLines(path)
                .stream()
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        data.addAll(lines);
    }
}
