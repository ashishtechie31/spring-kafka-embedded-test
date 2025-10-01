package com.learning.kafka.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MessagePublisher {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void publishMessage() {


    }
}
