package com.ugurukku.event.driven.microservice.twitter.to.kafka.service;

import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.init.StreamInitializer;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages = "com.ugurukku.event.driven.microservice")
@RequiredArgsConstructor
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    private final StreamRunner streamRunner;

    private final StreamInitializer streamInitializer;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("App starts..");
        streamInitializer.init();
        streamRunner.start();
    }
}
