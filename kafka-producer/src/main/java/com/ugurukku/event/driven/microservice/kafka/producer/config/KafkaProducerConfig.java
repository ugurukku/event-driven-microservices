package com.ugurukku.event.driven.microservice.kafka.producer.config;

import com.ugurukku.event.driven.microservice.config.KafkaConfigData;
import com.ugurukku.event.driven.microservice.config.KafkaProducerConfigData;
import lombok.AllArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Configuration
@AllArgsConstructor
public class KafkaProducerConfig<K extends Serializable, V extends SpecificRecordBase> {

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducerConfigData kafkaProducerConfigData;

    @Bean
    public Map<String,Object> producerConfig(){
        Map<String,Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG,kafkaConfigData.getBootstrapService());
        props.put(kafkaConfigData.getSchemaRegistryUrlKey(), kafkaConfigData.getSchemaRegistryUrl());
        props.put(KEY_SERIALIZER_CLASS_CONFIG,kafkaProducerConfigData.getValueSerializerClass());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG,kafkaProducerConfigData.getValueSerializerClass());
        props.put(BATCH_SIZE_CONFIG,kafkaProducerConfigData.getBatchSize() *
                kafkaProducerConfigData.getBatchSizeBoostFactor());
        props.put(LINGER_MS_CONFIG,kafkaProducerConfigData.getLingerMs());
        props.put(COMPRESSION_TYPE_CONFIG,kafkaProducerConfigData.getCompressionType());
        props.put(ACKS_CONFIG,kafkaProducerConfigData.getAcks());
        props.put(REQUEST_TIMEOUT_MS_CONFIG,kafkaProducerConfigData.getRequestTimeoutMs());
        props.put(RETRIES_CONFIG,kafkaProducerConfigData.getRetryCount());


        return props;
    }

    @Bean
    public ProducerFactory<K,V> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<K,V> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

}
