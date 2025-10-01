# Integration Test using Spring Boot Embedded Kafka Server (Gradle)

Minimal Spring Boot project (Java 17) demonstrating a Kafka producer and consumer using `spring-kafka`.
- Build with Gradle
- Producer endpoint: POST /api/publish
- Consumer listens to topic `sample-topic`
- Execute the test under shared folder with optimized approach
- Integration test using existing approach with delays.

Run Kafka locally (example using Docker Compose) or point `spring.kafka.bootstrap-servers` to your cluster.
Use conduktor to produce and consume messages.
Run Integration test case single class OR multiple classes.
1. EmployeeListenerTest
2. EmployeeServiceImplTest
