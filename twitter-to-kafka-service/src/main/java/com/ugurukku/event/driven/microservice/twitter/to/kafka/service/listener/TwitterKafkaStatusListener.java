package com.ugurukku.event.driven.microservice.twitter.to.kafka.service.listener;

import com.ugurukku.event.driven.microservice.config.KafkaConfigData;
import com.ugurukku.event.driven.microservice.kafka.producer.config.service.KafkaProducer;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import kafka.avro.model.TwitterAvroModel;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
@RequiredArgsConstructor
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    @Override
    public void onStatus(Status status) {
        LOGGER.info("Twitter status with text {} sending to kafka topic {}",
                status.getText(),
                kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(),twitterAvroModel.getUserId(),twitterAvroModel);
    }
}
