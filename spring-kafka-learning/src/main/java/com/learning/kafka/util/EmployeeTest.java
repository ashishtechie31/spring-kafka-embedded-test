package com.learning.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.kafka.model.Address;
import com.learning.kafka.model.Department;
import com.learning.kafka.model.Employee;

public class EmployeeTest {
    public static void main(String[] args) throws JsonProcessingException {

        Employee employee = new Employee();
        employee.setEmpId("1");
        employee.setSalary(123.00);
        employee.setEmpName("ashish");
        employee.setDateOfJoining("2025-12-12");

        Address address = new Address();
        address.setCity("pune");
        address.setStreet("testStreet");
        address.setState("MH");
        address.setPinCode("411057");
        address.setCountry("India");
        employee.setAddress(address);
        Department department = new Department();
        department.setDepartmentId("Dept1234");
        department.setDepartmentType("IT");
        department.setEmpId(employee.getEmpId());
        department.setAddress(employee.getAddress());
        employee.setDepartment(department);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(employee);
        System.out.println("Json" + json);
        String json1 = "{\n" +
                "  \"EmpId\": \"1\",\n" +
                "  \"EmpName\": \"ashish\",\n" +
                "  \"Salary\": 123,\n" +
                "  \"DateOfJoining\": \"2025-12-12\",\n" +
                "  \"Address\": {\n" +
                "    \"Street\": \"testStreet\",\n" +
                "    \"City\": \"pune\",\n" +
                "    \"PinCode\": \"411057\",\n" +
                "    \"State\": \"MH\",\n" +
                "    \"Country\": \"India\"\n" +
                "  }\n" +
                "}";
        Employee employee1 = objectMapper.readValue(json1, Employee.class);
        System.out.println("Nsame:" + employee1.getEmpName());

    }
}
