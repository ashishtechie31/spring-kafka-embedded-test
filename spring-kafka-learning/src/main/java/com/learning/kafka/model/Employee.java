package com.learning.kafka.model;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Setter
@NoArgsConstructor
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(value = PropertyNamingStrategies.UpperCamelCaseStrategy.class)
public class Employee {
    private String empId;
    private String empName;
    private Double salary;
    private String dateOfJoining;
    private Department department;

    public Department getDepartment() {
        return department;
    }

    public void setDepartment(Department department) {
        this.department = department;
    }

    public String getEmpId() {
        return empId;
    }

    private Address address;

    public Address getAddress() {
        return address;
    }

    public String getDateOfJoining() {
        return dateOfJoining;
    }

    public Double getSalary() {
        return salary;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpId(String empId) {
        this.empId = empId;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    public void setSalary(Double salary) {
        this.salary = salary;
    }

    public void setDateOfJoining(String dateOfJoining) {
        this.dateOfJoining = dateOfJoining;
    }

    public void setAddress(Address address) {
        this.address = address;
    }
}
