package com.learning.kafka.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.kafka.model.Employee;
import com.learning.kafka.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static com.learning.kafka.constant.ApplicationConstant.EMPLOYEE_DEPT_SEND_EVENT;

@Component
public class EmployeeListener {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmployeeService employeeService;

    @KafkaListener(topics = "employee-send-detail-event", groupId = "employee-group")
    public void listenEmployeeEvent(String employeeEvent,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
                                    @Header(KafkaHeaders.RECEIVED_KEY) final String receivedKey) throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        Employee employee = objectMapper.readValue(employeeEvent, Employee.class);

        employeeService.processEmployeeRequest(employee);
        kafkaTemplate.send(EMPLOYEE_DEPT_SEND_EVENT, UUID.randomUUID().toString(), "empData");
    }
}
