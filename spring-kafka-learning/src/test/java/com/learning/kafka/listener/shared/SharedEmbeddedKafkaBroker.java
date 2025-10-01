package com.learning.kafka.listener.shared;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SharedEmbeddedKafkaBroker {
    private static final String[] TOPICS = {
            "employee-send-detail-event",
            "department-send-detail-event",
            "employee-dept-send-event",
            "audit-db-event"
    };

    private static EmbeddedKafkaBroker broker;

    public static synchronized EmbeddedKafkaBroker getBroker() {
        if (broker == null) {
            broker = new EmbeddedKafkaZKBroker(1, true, 1, TOPICS)
                    .kafkaPorts(0);
            broker.afterPropertiesSet(); // start broker
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
                    broker.destroy()));  // stop broker on JVM shutdown
        }
        return broker;
    }
}
