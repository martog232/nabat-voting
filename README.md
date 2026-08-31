# Nabat Voting

A Spring Boot 3.4 / Java 21 microservice that handles credibility voting for real-time safety alerts.
Votes are published as Kafka events, allowing downstream services to maintain a live credibility
projection for each alert.

## Architecture

The service follows a **hexagonal (ports-and-adapters)** architecture:

```
src/main/java/com/example/nabatvoting/
├── domain/
│   ├── model/          # Vote, VoteId, AlertId, VoterId
│   ├── event/          # VoteCastEvent, VoteRemovedEvent, VoteTallies
│   └── port/
│       ├── in/         # CastVoteUseCase, CastVoteCommand  (inbound ports)
│       └── out/        # VoteRepository, VoteEventPublisher (outbound ports)
├── application/
│   ├── service/        # CastVoteService (use-case implementation)
│   └── projection/     # CredibilityProjection (read model)
└── infrastructure/
    ├── kafka/          # KafkaVoteEventConsumer, KafkaTopics, VoteChangedEvents, VoteEventAvroSerializer
    ├── outbox/         # OutboxVoteEventPublisher, OutboxRelay (events leave via a table)
    ├── persistence/    # PostgresVoteRepositoryAdapter, VoteJpaEntity, VoteJpaRepository
    └── config/         # KafkaConfig, SchedulingConfig
```

See [docs/architecture.md](docs/architecture.md) for a detailed description.

## Quick Start

### Prerequisites

| Tool | Version |
|------|---------|
| Java | 21+ |
| Apache Kafka | 3.x |
| Maven | 3.9+ |

### Running locally

1. Start Kafka:
   ```bash
   # Using Docker
   docker run -d --name kafka \
     -p 9092:9092 \
     -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
     bitnami/kafka:latest
   ```

2. Run the service:
   ```bash
   ./mvnw spring-boot:run
   ```

### Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `spring.kafka.consumer.group-id` | `nabat-voting-group` | Consumer group for projection updates |
| `nabat.schema-registry.url` | `http://127.0.0.1:8085` | Where the Avro schema for `vote.changed` lives. Required: without it a vote cannot be serialised. Tests use `mock://` |
| `nabat.outbox.poll-interval` | `PT1S` | How often the relay looks for events to send |
| `nabat.outbox.batch-size` | `100` | Rows claimed per pass |
| `nabat.outbox.send-timeout` | `PT10S` | A slower send counts as failed and is retried |
| `nabat.outbox.retention` | `P7D` | How long published rows are kept before being purged |
| `nabat.outbox.purge-interval` | `PT1H` | How often the purge runs |

Events are not sent to Kafka directly. They are written to `outbox_event` in the same transaction
as the vote, and `OutboxRelay` sends the committed rows — see
[docs/architecture.md](docs/architecture.md) for why.

## Testing

```bash
./mvnw test
```

Tests use an embedded Kafka broker (via `@EmbeddedKafka`) — **no external Kafka is required** to run
the tests.

## Kafka Events

See [docs/kafka-events.md](docs/kafka-events.md) for the full event schema and topic documentation.
