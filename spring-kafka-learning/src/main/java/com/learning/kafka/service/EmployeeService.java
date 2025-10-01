package com.learning.kafka.service;

import com.learning.kafka.model.Employee;

public interface EmployeeService {

    void processEmployeeRequest(Employee employee);
}
