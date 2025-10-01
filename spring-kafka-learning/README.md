# Spring Boot Kafka Sample (Gradle)

Minimal Spring Boot project (Java 17) demonstrating a Kafka producer and consumer using `spring-kafka`.
- Build with Gradle
- Producer endpoint: POST /api/publish
- Consumer listens to topic `sample-topic`

Run Kafka locally (example using Docker Compose) or point `spring.kafka.bootstrap-servers` to your cluster.
Use conduktor to produce and consume messages.
Run Component test case single class and multiple classes.
1. EmployeeListenerTest
2. EmployeeServiceImplTest