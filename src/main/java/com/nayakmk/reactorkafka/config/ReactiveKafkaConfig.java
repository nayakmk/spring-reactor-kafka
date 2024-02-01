package com.nayakmk.reactorkafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ReactiveKafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveKafkaConfig.class);

    private String topic = "test-topic";

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties();
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CustomPartitionAssigner.class.getName());
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(consumerProps);
        ReceiverOptions<String, String> addedAssignListener = receiverOptions.subscription(Collections.singleton(topic)).addAssignListener(partitions -> logger.info("Assigned partitions: {}", partitions));
        return new ReactiveKafkaConsumerTemplate<>(addedAssignListener);
    }
}
