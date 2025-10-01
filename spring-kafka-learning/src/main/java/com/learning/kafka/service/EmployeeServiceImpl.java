package com.learning.kafka.service;

import com.learning.kafka.model.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.UUID;

import static com.learning.kafka.constant.ApplicationConstant.AUDIT_DB_EVENT;

@Service
@Flogger
@RequiredArgsConstructor
public class EmployeeServiceImpl implements EmployeeService {

    @Autowired
    private DepartmentService departmentService;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Override
    public void processEmployeeRequest(Employee employee) {

        String deptId = employee.getDepartment().getDepartmentId();

        if (!StringUtils.hasText(deptId) || deptId.startsWith("0")) {
            throw new IllegalArgumentException("Invalid Dept Id" + deptId);
        }
        else if (deptId.equalsIgnoreCase("IT")) {
            kafkaTemplate.send(AUDIT_DB_EVENT, "IT", employee);
        }
        if (deptId.equalsIgnoreCase("Marketing")) {
            kafkaTemplate.send(AUDIT_DB_EVENT, "Marketing", employee);
        }
        kafkaTemplate.send(AUDIT_DB_EVENT, UUID.randomUUID().toString(), employee);
    }
}

