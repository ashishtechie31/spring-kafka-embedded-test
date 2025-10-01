package com.learning.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class ConsumerListener {

    // Shared queue for testing
    public static final BlockingQueue<Object> messages = new LinkedBlockingQueue<>();

    @Autowired
    private KafkaTemplate kafkaTemplate;
    @KafkaListener(topics = "sample-topic", groupId = "sample-group")
    public void listen(ConsumerRecord<String, Object> record, @Payload Object payload) {
        System.out.println("Received message: key=" + record.key() + " value=" + payload);
        messages.offer(payload);
        kafkaTemplate.send("employee-topic","empid","empData");
    }
}
