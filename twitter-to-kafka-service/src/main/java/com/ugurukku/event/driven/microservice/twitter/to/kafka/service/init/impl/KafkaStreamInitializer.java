package com.ugurukku.event.driven.microservice.twitter.to.kafka.service.init.impl;

import com.ugurukku.event.driven.microservice.config.KafkaConfigData;
import com.ugurukku.event.driven.microservice.kafka.admin.client.KafkaAdminClient;
import com.ugurukku.event.driven.microservice.kafka.admin.config.KafkaAdminConfig;
import com.ugurukku.event.driven.microservice.twitter.to.kafka.service.init.StreamInitializer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaStreamInitializer implements StreamInitializer {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamInitializer.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaAdminClient kafkaAdminClient;

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        LOGGER.info("Topics with name {} is ready for operations",kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
