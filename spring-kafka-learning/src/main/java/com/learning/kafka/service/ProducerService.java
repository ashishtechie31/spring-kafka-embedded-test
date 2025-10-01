package com.learning.kafka.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic = "sample-topic";

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Map<String, Object> payload) {
        kafkaTemplate.send(topic, payload);
    }
}
