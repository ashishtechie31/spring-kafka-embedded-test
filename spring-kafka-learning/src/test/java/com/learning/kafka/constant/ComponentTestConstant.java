package com.learning.kafka.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ComponentTestConstant {

    public static final String EMPLOYEE_TOPIC = "employee-send-detail-event";

    public static final String EMPLOYEE_DEPT_TOPIC = "employee-dept-send-event";

    public static final String KAFKA_MESSAGE_KEY = "kafka_receivedMessageKey";
}
