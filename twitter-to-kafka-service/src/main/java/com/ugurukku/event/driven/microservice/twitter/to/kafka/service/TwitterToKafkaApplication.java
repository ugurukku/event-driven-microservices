package com.ugurukku.event.driven.microservice.twitter.to.kafka.service;

import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.config.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    private final ConfigData configData;

    public TwitterToKafkaApplication(ConfigData configData) {
        this.configData = configData;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info(configData.getWelcomeMessage());
        LOGGER.info("App starts..");
        LOGGER.info(configData.getTwitterKeywords().toString());
    }
}
