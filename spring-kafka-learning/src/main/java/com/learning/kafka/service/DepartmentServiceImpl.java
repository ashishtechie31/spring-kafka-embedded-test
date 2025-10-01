package com.learning.kafka.service;

import com.learning.kafka.model.Department;
import lombok.RequiredArgsConstructor;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Flogger
public class DepartmentServiceImpl implements DepartmentService {

    @Override
    public void processDepartmentRequest(Department department) {

    }
}
