package com.learning.kafka.listener;

import lombok.extern.flogger.Flogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.learning.kafka.constant.ApplicationConstant.EMPLOYEE_DEPT_SEND_EVENT;

@Component
@Flogger
public class DepartmentListener {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = "department-send-detail-event", groupId = "department-group")
    public void listenDepartmentEvent(ConsumerRecord<String, Object> record, @Payload Object payload) {
        
        kafkaTemplate.send(EMPLOYEE_DEPT_SEND_EVENT, "deptId", "deptData");
    }
}
