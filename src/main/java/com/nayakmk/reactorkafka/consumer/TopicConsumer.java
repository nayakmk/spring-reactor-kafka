package com.nayakmk.reactorkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TopicConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TopicConsumer.class);

    @Autowired
    ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;

    @PostConstruct
    public void startConsumer() {
        Flux<ReceiverRecord<String, String>> kafkaFlux = reactiveKafkaConsumerTemplate.receive();
        kafkaFlux.subscribe(record -> {
            logger.info("Received message: key={}, value={}, from topic={}, partition={}, offset={}",
                    record.key(), record.value(), record.topic(), record.partition(), record.offset());
        }, error -> logger.error("Error consuming messages", error),
                () -> logger.info("Completed"));
    }
}
