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
│   ├── event/          # VoteCastEvent
│   └── port/
│       ├── in/         # CastVoteUseCase, CastVoteCommand  (inbound ports)
│       └── out/        # VoteRepository, VoteEventPublisher (outbound ports)
├── application/
│   ├── service/        # CastVoteService (use-case implementation)
│   └── projection/     # CredibilityProjection (read model)
└── infrastructure/
    ├── kafka/          # KafkaVoteEventPublisher, KafkaVoteEventConsumer, KafkaTopics
    ├── persistence/    # PostgresVoteRepositoryAdapter, VoteJpaEntity, VoteJpaRepository
    └── config/         # KafkaConfig
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

## Testing

```bash
./mvnw test
```

Tests use an embedded Kafka broker (via `@EmbeddedKafka`) — **no external Kafka is required** to run
the tests.

## Kafka Events

See [docs/kafka-events.md](docs/kafka-events.md) for the full event schema and topic documentation.
