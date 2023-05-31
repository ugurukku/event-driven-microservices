package com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.impl;

import com.ugurukku.event.driven.microservice.config.ConfigData;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "false",matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final ConfigData configData;

    private final TwitterKafkaStatusListener listener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(ConfigData configData, TwitterKafkaStatusListener listener) {
        this.configData = configData;
        this.listener = listener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = TwitterStreamFactory.getSingleton();
        twitterStream.addListener(listener);
        System.out.println(twitterStream.getAuthorization());
        filter();
    }

    @PreDestroy
    public void shutDown(){
        if (twitterStream != null){
            LOGGER.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }

    private void filter() {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOGGER.info("Started filtering stream for keywords {}", Arrays.toString(keywords));
    }
}
