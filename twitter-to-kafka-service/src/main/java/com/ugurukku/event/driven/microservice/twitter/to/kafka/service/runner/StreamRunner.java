package com.ugurukku.event.driven.microservice.twitter.to.kafka.service.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;
}
