package com.ugurukku.event.driven.microservice.twitter.to.kafka.service;

import com.ugurukku.event.driven.microservice.config.ConfigData;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages = "com.ugurukku.event.driven.microservice")
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    private final StreamRunner streamRunner;

    private final ConfigData configData;

    public TwitterToKafkaApplication(StreamRunner streamRunner, ConfigData configData) {
        this.streamRunner = streamRunner;
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
        streamRunner.start();
    }
}
