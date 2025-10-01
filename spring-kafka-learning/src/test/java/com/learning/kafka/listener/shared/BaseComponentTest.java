package com.learning.kafka.listener.shared;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;

/*Test Case has been written with new approach using shared kafka broker
* it used the single kafka port */
@Disabled
@ActiveProfiles("component")
@AutoConfigureMockMvc
@SpringBootTest
public class BaseComponentTest {

    protected static final String KAFKA_RECEIVED_MESSAGE_KEY = "kafka_receivedMessageKey";
    protected static final String TEST_RECEIVED_KEY = "test-received-key";

    protected static final EmbeddedKafkaBroker embeddedKafkaBroker = SharedEmbeddedKafkaBroker.getBroker();

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", embeddedKafkaBroker::getBrokersAsString);
    }

    protected Consumer<String, String> consumeMessageFromTopic(String topicName) {
        var kafkaEventConsumer = getKafkaConsumer(topicName);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaEventConsumer, topicName);
        return kafkaEventConsumer;
    }
    protected Consumer<String, String> aConsumerReadMsgEnd(String topicName) {
        var kafkaEventConsumer = getKafkaConsumer(topicName);
        embeddedKafkaBroker.consumeFromEmbeddedTopics(kafkaEventConsumer, true, topicName);
        return kafkaEventConsumer;
    }

    protected void verifyConsumedMessageCount(Consumer<String, String> kafkaEventConsumer, Matcher<Integer> matcher) {
        var receivedEventMessages = getRecords(kafkaEventConsumer, Duration.ofMillis(5000), 10);
        assertThat(receivedEventMessages.count(), matcher);
    }

    protected KafkaProducer<String, String> sendMessageToTopic(String topic, String message) {
        var kafkaProducer = getKafkaProducer();
        var producerRecord = new ProducerRecord<String, String>(topic, message);
        producerRecord.headers().add(KAFKA_RECEIVED_MESSAGE_KEY, TEST_RECEIVED_KEY.getBytes());
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
        return kafkaProducer;
    }

    private KafkaProducer<String, String> getKafkaProducer() {
        var senderProperties = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        return new KafkaProducer<>(senderProperties);
    }

    private Consumer<String, String> getKafkaConsumer(String topicName) {
        Map<String, Object> consumerProps =
                consumerProps(UUID.randomUUID().toString(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
        consumer.subscribe(List.of(topicName));
        return consumer;
    }
}
