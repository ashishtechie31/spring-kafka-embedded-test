package com.learning.kafka.listener;

import com.learning.kafka.IntroduceDelay;
import com.learning.kafka.IntroduceDelayExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;

import static com.learning.kafka.constant.ApplicationConstant.AUDIT_DB_EVENT;
import static com.learning.kafka.constant.ApplicationConstant.EMPLOYEE_DEPT_SEND_EVENT;
import static com.learning.kafka.constant.ComponentTestConstant.EMPLOYEE_DEPT_TOPIC;
import static com.learning.kafka.constant.ComponentTestConstant.EMPLOYEE_TOPIC;
import static com.learning.kafka.constant.ComponentTestConstant.KAFKA_MESSAGE_KEY;
import static com.learning.kafka.util.ComponentTestUtil.getKafkaConsumer;
import static com.learning.kafka.util.ComponentTestUtil.getKafkaProducer;
import static com.learning.kafka.util.ComponentTestUtil.readFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;


@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
@EmbeddedKafka(partitions = 1,
        topics = {"employee-send-detail-event", "employee-dept-send-event",
                "audit-db-event"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@ActiveProfiles("component")
@AutoConfigureMockMvc
@ExtendWith(IntroduceDelayExtension.class)
class EmployeeListenerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    @IntroduceDelay(duration = 5000)
    void testProcessEmployeeRequest() {
        String message = readFile("employee-event.json");
        final var kafkaProducer = getKafkaProducer(embeddedKafkaBroker);
        var producerRecord = new ProducerRecord<String, String>(EMPLOYEE_TOPIC, message);
        producerRecord.headers().add(KAFKA_MESSAGE_KEY, RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);

        var kafkaConsumer = getKafkaConsumer(EMPLOYEE_TOPIC, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaConsumer, EMPLOYEE_TOPIC);
        final var receivedMessages = getRecords(kafkaConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessages.count()).isEqualTo(1);

        var kafkaDeptConsumer = getKafkaConsumer(EMPLOYEE_DEPT_SEND_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaDeptConsumer, EMPLOYEE_DEPT_SEND_EVENT);
        final var receivedMessagesDept = getRecords(kafkaDeptConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessagesDept.count()).isEqualTo(1);

        for (ConsumerRecord<String, String> consumerRecord : receivedMessagesDept.records(EMPLOYEE_DEPT_TOPIC)) {
            System.out.println("Key:: " + consumerRecord.key() + " Value:: " + consumerRecord.value());
        }

        var kafkaAuditConsumer = getKafkaConsumer(AUDIT_DB_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaAuditConsumer, AUDIT_DB_EVENT);
        final var receivedAuditMessage = getRecords(kafkaAuditConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedAuditMessage.count()).isEqualTo(1);
    }

    @Test
    @IntroduceDelay(duration = 5000)
    void testProcessEmployeeRequestInvalidDeptId() {
        String message = readFile("employee-event-dept-invalidDeptId.json");
        final var kafkaProducer = getKafkaProducer(embeddedKafkaBroker);
        var producerRecord = new ProducerRecord<String, String>(EMPLOYEE_TOPIC, message);
        producerRecord.headers().add(KAFKA_MESSAGE_KEY, RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);

        var kafkaConsumer = getKafkaConsumer(EMPLOYEE_TOPIC, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaConsumer, EMPLOYEE_TOPIC);
        final var receivedMessages = getRecords(kafkaConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessages.count()).isEqualTo(1);

        var kafkaDeptConsumer = getKafkaConsumer(EMPLOYEE_DEPT_SEND_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaDeptConsumer, EMPLOYEE_DEPT_SEND_EVENT);
        final var receivedMessagesDept = getRecords(kafkaDeptConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessagesDept.count()).isZero();

        for (ConsumerRecord<String, String> consumerRecord : receivedMessagesDept.records(EMPLOYEE_DEPT_SEND_EVENT)) {
            System.out.println("Key:: " + consumerRecord.key() + " Value:: " + consumerRecord.value());
        }

        var kafkaAuditConsumer = getKafkaConsumer(AUDIT_DB_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaAuditConsumer, AUDIT_DB_EVENT);
        final var receivedAuditMessage = getRecords(kafkaAuditConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedAuditMessage.count()).isZero();
    }

    @Test
    @IntroduceDelay(duration = 5000)
    void testProcessEmployeeRequestMarketing() {
        String message = readFile("employee-event-dept-marketing.json");
        final var kafkaProducer = getKafkaProducer(embeddedKafkaBroker);
        var producerRecord = new ProducerRecord<String, String>(EMPLOYEE_TOPIC, message);
        producerRecord.headers().add(KAFKA_MESSAGE_KEY, RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);

        var kafkaConsumer = getKafkaConsumer(EMPLOYEE_TOPIC, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaConsumer, EMPLOYEE_TOPIC);
        final var receivedMessages = getRecords(kafkaConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessages.count()).isEqualTo(1);

        var kafkaDeptConsumer = getKafkaConsumer(EMPLOYEE_DEPT_SEND_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaDeptConsumer, EMPLOYEE_DEPT_SEND_EVENT);
        final var receivedMessagesDept = getRecords(kafkaDeptConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessagesDept.count()).isEqualTo(1);

        for (ConsumerRecord<String, String> consumerRecord : receivedMessagesDept.records(EMPLOYEE_DEPT_SEND_EVENT)) {
            System.out.println("Key:: " + consumerRecord.key() + " Value:: " + consumerRecord.value());
        }

        var kafkaAuditConsumer = getKafkaConsumer(AUDIT_DB_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaAuditConsumer, AUDIT_DB_EVENT);
        final var receivedAuditMessage = getRecords(kafkaAuditConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedAuditMessage.count()).isEqualTo(1);
    }

    @Test
    @IntroduceDelay(duration = 5000)
    void testProcessEmployeeRequestDeptNull() {
        String message = readFile("employee-event-dept-null.json");
        final var kafkaProducer = getKafkaProducer(embeddedKafkaBroker);
        var producerRecord = new ProducerRecord<String, String>(EMPLOYEE_TOPIC, message);
        producerRecord.headers().add(KAFKA_MESSAGE_KEY, RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);

        var kafkaConsumer = getKafkaConsumer(EMPLOYEE_TOPIC, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaConsumer, EMPLOYEE_TOPIC);
        final var receivedMessages = getRecords(kafkaConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessages.count()).isEqualTo(1);

        var kafkaDeptConsumer = getKafkaConsumer(EMPLOYEE_DEPT_SEND_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaDeptConsumer, EMPLOYEE_DEPT_SEND_EVENT);
        final var receivedMessagesDept = getRecords(kafkaDeptConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessagesDept.count()).isZero();

        for (ConsumerRecord<String, String> consumerRecord : receivedMessagesDept.records(EMPLOYEE_DEPT_TOPIC)) {
            System.out.println("Key:: " + consumerRecord.key() + " Value:: " + consumerRecord.value());
        }

        var kafkaAuditConsumer = getKafkaConsumer(AUDIT_DB_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaAuditConsumer, AUDIT_DB_EVENT);
        final var receivedAuditMessage = getRecords(kafkaAuditConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedAuditMessage.count()).isZero();
    }

    @Test
    @IntroduceDelay(duration = 5000)
    void testProcessEmployeeRequestIt() {
        String message = readFile("employee-event-it.json");
        final var kafkaProducer = getKafkaProducer(embeddedKafkaBroker);
        var producerRecord = new ProducerRecord<String, String>(EMPLOYEE_TOPIC, message);
        producerRecord.headers().add(KAFKA_MESSAGE_KEY, RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);

        var kafkaConsumer = getKafkaConsumer(EMPLOYEE_TOPIC, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaConsumer, EMPLOYEE_TOPIC);
        final var receivedMessages = getRecords(kafkaConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessages.count()).isEqualTo(1);

        var kafkaDeptConsumer = getKafkaConsumer(EMPLOYEE_DEPT_SEND_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaDeptConsumer, EMPLOYEE_DEPT_SEND_EVENT);
        final var receivedMessagesDept = getRecords(kafkaDeptConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedMessagesDept.count()).isEqualTo(1);

        for (ConsumerRecord<String, String> consumerRecord : receivedMessagesDept.records(EMPLOYEE_DEPT_SEND_EVENT)) {
            System.out.println("Key:: " + consumerRecord.key() + " Value:: " + consumerRecord.value());
        }

        var kafkaAuditConsumer = getKafkaConsumer(AUDIT_DB_EVENT, embeddedKafkaBroker);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaAuditConsumer, AUDIT_DB_EVENT);
        final var receivedAuditMessage = getRecords(kafkaAuditConsumer, Duration.ofMillis(5000), 1);
        assertThat(receivedAuditMessage.count()).isEqualTo(1);
    }
}
