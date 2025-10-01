package com.learning.kafka.listener.shared;

import org.junit.jupiter.api.Test;

import static com.learning.kafka.constant.ApplicationConstant.AUDIT_DB_EVENT;
import static com.learning.kafka.constant.ApplicationConstant.EMPLOYEE_DEPT_SEND_EVENT;
import static com.learning.kafka.constant.ComponentTestConstant.EMPLOYEE_TOPIC;
import static com.learning.kafka.util.ComponentTestUtil.readFile;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;


class EmployeeServiceImplTest extends BaseComponentTest {

    @Test
    void testProcessEmployeeRequest() {
        String message = readFile("employee-event.json");

        sendMessageToTopic(EMPLOYEE_TOPIC, message);

        var kafkaConsumer = consumeMessageFromTopic(EMPLOYEE_TOPIC);

        verifyConsumedMessageCount(kafkaConsumer, greaterThanOrEqualTo(1));

        var kafkaEmployeeConsumer = consumeMessageFromTopic(EMPLOYEE_DEPT_SEND_EVENT);

        verifyConsumedMessageCount(kafkaEmployeeConsumer, greaterThanOrEqualTo(1));

        var kafkaAuditConsumer = consumeMessageFromTopic(AUDIT_DB_EVENT);

        verifyConsumedMessageCount(kafkaAuditConsumer, greaterThanOrEqualTo(1));
    }

    @Test
    void testProcessEmployeeRequestInvalidDeptId() {

        String message = readFile("employee-event-dept-invalidDeptId.json");

        sendMessageToTopic(EMPLOYEE_TOPIC, message);

        var kafkaConsumer = consumeMessageFromTopic(EMPLOYEE_TOPIC);

        verifyConsumedMessageCount(kafkaConsumer, greaterThanOrEqualTo(1));

        var kafkaEmployeeConsumer = aConsumerReadMsgEnd(EMPLOYEE_DEPT_SEND_EVENT);

        verifyConsumedMessageCount(kafkaEmployeeConsumer, lessThanOrEqualTo(0));

        var kafkaAuditConsumer = aConsumerReadMsgEnd(AUDIT_DB_EVENT);

        verifyConsumedMessageCount(kafkaAuditConsumer, lessThanOrEqualTo(0));
    }

    @Test
    void testProcessEmployeeRequestMarketing() {

        String message = readFile("employee-event-dept-marketing.json");

        sendMessageToTopic(EMPLOYEE_TOPIC, message);

        var kafkaConsumer = consumeMessageFromTopic(EMPLOYEE_TOPIC);

        verifyConsumedMessageCount(kafkaConsumer, greaterThanOrEqualTo(1));

        var kafkaEmployeeConsumer = consumeMessageFromTopic(EMPLOYEE_DEPT_SEND_EVENT);

        verifyConsumedMessageCount(kafkaEmployeeConsumer, greaterThanOrEqualTo(1));

        var kafkaAuditConsumer = consumeMessageFromTopic(AUDIT_DB_EVENT);

        verifyConsumedMessageCount(kafkaAuditConsumer, greaterThanOrEqualTo(1));
    }

    @Test
    void testProcessEmployeeRequestDeptNull() {
        String message = readFile("employee-event-dept-null.json");

        sendMessageToTopic(EMPLOYEE_TOPIC, message);

        var kafkaConsumer = consumeMessageFromTopic(EMPLOYEE_TOPIC);

        verifyConsumedMessageCount(kafkaConsumer, greaterThanOrEqualTo(1));

        var kafkaEmployeeConsumer = consumeMessageFromTopic(EMPLOYEE_DEPT_SEND_EVENT);

        verifyConsumedMessageCount(kafkaEmployeeConsumer, greaterThanOrEqualTo(0));

        var kafkaAuditConsumer = consumeMessageFromTopic(AUDIT_DB_EVENT);

        verifyConsumedMessageCount(kafkaAuditConsumer, greaterThanOrEqualTo(0));
    }

    @Test
    void testProcessEmployeeRequestIt() {
        String message = readFile("employee-event-it.json");

        sendMessageToTopic(EMPLOYEE_TOPIC, message);

        var kafkaConsumer = consumeMessageFromTopic(EMPLOYEE_TOPIC);

        verifyConsumedMessageCount(kafkaConsumer, greaterThanOrEqualTo(1));

        var kafkaEmployeeConsumer = consumeMessageFromTopic(EMPLOYEE_DEPT_SEND_EVENT);

        verifyConsumedMessageCount(kafkaEmployeeConsumer, greaterThanOrEqualTo(1));

        var kafkaAuditConsumer = consumeMessageFromTopic(AUDIT_DB_EVENT);

        verifyConsumedMessageCount(kafkaAuditConsumer, greaterThanOrEqualTo(1));
    }
}
