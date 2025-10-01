package com.learning.kafka.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.Files.readAllBytes;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.util.ResourceUtils.getFile;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class ComponentTestUtil {

    public static String readFile(String fileName) {
        try {
            return new String(readAllBytes(getFile("classpath:" + fileName).toPath()));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return null;
    }

    public static KafkaProducer<String, String> getKafkaProducer(EmbeddedKafkaBroker embeddedKafkaBroker) {
        var senderProperties = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        return new KafkaProducer<>(senderProperties);
    }

    public static Consumer<String, String> getKafkaConsumer(final String topicName,
                                                            final EmbeddedKafkaBroker embeddedKafkaBroker) {
        Map<String, Object> consumerProps =
                consumerProps(UUID.randomUUID().toString(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
        consumer.subscribe(List.of(topicName));
        return consumer;
    }

    public static void waitForExecution(final long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }
    }
}
