package com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.impl;

import com.ugurukku.event.driven.microservice.config.ConfigData;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;


@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final ConfigData configData;

    private final TwitterKafkaStatusListener listener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[]{
            "Mahesh Chand",
            "Jeff Prosise",
            "Dave McCarter",
            "Allen O'neill",
            "Monica Rathbun",
            "Henry He",
            "Raj Kumar",
            "Mark Prime",
            "Rose Tracey",
            "Mike Crown"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(ConfigData configData, TwitterKafkaStatusListener listener) {
        this.configData = configData;
        this.listener = listener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = configData.getMockMinTweetLength();
        int maxTweetLength = configData.getMockMaxTweetLength();
        long sleepTimeMs = configData.getMockSleepMs();

        LOGGER.info("Started mock filtering stream for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) throws TwitterException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        Thread thread = new Thread(() -> {

                try {
                    while (true) {
                        String formattedTweet = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                        Status status = TwitterObjectFactory.createStatus(formattedTweet);
                        listener.onStatus(status);
                        sleep(sleepTimeMs);
                    }
                } catch (TwitterException e) {
                    LOGGER.error("Error creating  twitter status", e);
                }

            });
            thread.start();

    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while sleeping for new status");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT,Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        return formatTweetAsJsonWithParams(params);
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;

        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constractRandomTweet(keywords, tweet, tweetLength);
    }

    private static String constractRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
