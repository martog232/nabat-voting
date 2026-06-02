# Kafka Events

## Topics

| Topic | Key | Value type | Partitions | Description |
|-------|-----|------------|------------|-------------|
| `vote.cast` | `alertId` | `VoteCastEvent` (JSON) | 1 | Published whenever a vote is cast on an alert |

## VoteCastEvent Schema

```json
{
  "voteId":   "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "alertId":  "alert-123",
  "voterId":  "user-456",
  "positive": true,
  "castAt":   "2024-11-15T10:30:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `voteId` | UUID (string) | Unique identifier of the vote |
| `alertId` | string | Identifier of the alert being voted on |
| `voterId` | string | Identifier of the voter |
| `positive` | boolean | `true` = credible, `false` = not credible |
| `castAt` | ISO-8601 timestamp | UTC time at which the vote was cast |

## Serialisation

Events are serialised to JSON using Jackson (`JsonSerializer` / `JsonDeserializer` from
`spring-kafka`).  The `VoteCastEvent` Java type is a record, so Jackson uses its compact
canonical constructor for deserialisation.

The consumer factory is configured to trust the package `com.example.nabatvoting.*` via
`JsonDeserializer#addTrustedPackages`.

## Consumer Group

The voting service subscribes to `vote.cast` with the consumer group
`nabat-voting-group` (configurable via `spring.kafka.consumer.group-id`).

If additional microservices need to react to vote events (e.g. a notification service), they
should use their own consumer group so that each service receives all messages independently.

## Partitioning

All events for a given alert are published with `alertId` as the Kafka message key.  This ensures
that votes for the same alert are always delivered in order to the same partition / consumer
instance.

## Local Development

### Running Kafka with Docker Compose

```yaml
# docker-compose.yml
services:
  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092
      KAFKA_CFG_ZOOKEEPER_CONNECT: ""
      KAFKA_KRAFT_CLUSTER_ID: kraft-local
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

```bash
docker compose up -d
./mvnw spring-boot:run
```

## Testing

Tests use the in-process `@EmbeddedKafka` broker provided by `spring-kafka-test`.  The embedded
broker is started automatically when a test class is annotated with `@EmbeddedKafka`, and the
`spring.kafka.bootstrap-servers` property is set to `${spring.embedded.kafka.brokers}` in
`src/test/resources/application.yaml` so that both the producer and consumer factories point to
the embedded broker.

No external Kafka installation is required to run the tests.
