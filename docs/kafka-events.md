# Kafka Events

## Topics

| Topic | Key | Value type | Partitions | Description |
|-------|-----|------------|------------|-------------|
| `vote.cast` | `alertId` | `VoteCastEvent` (JSON) | 1 | Published whenever a vote is cast or changed |
| `vote.removed` | `alertId` | `VoteRemovedEvent` (JSON) | 1 | Published when a voter retracts their vote |

## VoteCastEvent Schema

```json
{
  "voteId":   "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "alertId":  "alert-123",
  "voterId":  "user-456",
  "voteType": "CONFIRM",
  "castAt":   "2024-11-15T10:30:00Z",
  "tallies":  {
    "upvotes":         3,
    "downvotes":       1,
    "confirmations":   2,
    "credibilityScore": 6
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `voteId` | UUID (string) | Unique identifier of the vote |
| `alertId` | string | Identifier of the alert being voted on |
| `voterId` | string | Identifier of the voter |
| `voteType` | `UPVOTE` \| `DOWNVOTE` \| `CONFIRM` | What was cast |
| `castAt` | ISO-8601 timestamp | UTC time at which the vote was cast |
| `tallies` | object | The alert's counts **as of this vote**, read from the write model in the same transaction |

## VoteRemovedEvent Schema

Same `tallies`, with `removedAt` in place of `castAt` and no `voteId` or `voteType` — the
projection only needs to know which alert changed and to what.

## Why the tallies are on the event

A consumer that keeps its own projection would otherwise have to apply a delta, which
double-counts the second time an event is delivered, or call back for the stats, which is a
synchronous hop onto a projection that is itself asynchronous. Absolute values make applying
the event idempotent: writing them twice is the same write.

`credibilityScore` is derived (`upvotes - downvotes + 2 × confirmations`) but travels
anyway, because `VoteCounts` is the single definition of that formula and a consumer
recomputing it would be a copy free to drift.

Ordering is per alert, since that is the message key. Two events for one alert arrive in the
order they were written; nothing depends on ordering *between* alerts.

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

## Delivery guarantees

Events are not sent from the transaction that writes the vote.  They are written to the
`outbox_event` table in that transaction and sent afterwards by `OutboxRelay`, which reads only
committed rows — so an event can never describe a vote that is not there, and a crash before the
send does not lose it.

The price is at-least-once delivery: the send and the row's `published_at` update are not atomic,
so a crash between them re-sends.  Consumers must be idempotent.  This service's own consumer
recomputes each alert's counts from the write model rather than applying deltas, which is what
makes a redelivery harmless.

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
