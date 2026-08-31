# Kafka Events

## Topics

| Topic | Key | Value type | Partitions | Description |
|-------|-----|------------|------------|-------------|
| `vote.changed` | `alertId` | `VoteChanged` (Avro) | 1 | Every change to an alert's votes: cast, change of mind, retraction |

## VoteChanged Schema

The schema is [`src/main/avro/VoteChanged.avsc`](../src/main/avro/VoteChanged.avsc) and it is
the contract, not a description of one: `avro-maven-plugin` generates the Java class from it,
so the code cannot drift from the schema. nabat-app holds a verbatim copy of the same file and
generates its own class from it; the registry is what makes a divergence between the two
copies fail instead of drift.

Written as JSON, a message looks like this — but it does not travel as JSON:

```json
{
  "changeType": "CAST",
  "voteId":     "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "alertId":    "alert-123",
  "voterId":    "user-456",
  "voteType":   "CONFIRM",
  "occurredAt": 1731666600000,
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
| `changeType` | enum `CAST` \| `REMOVED` \| `UNKNOWN` | What happened |
| `voteId` | nullable string | The vote; null on a retraction |
| `alertId` | string | Identifier of the alert being voted on |
| `voterId` | string | Identifier of the voter |
| `voteType` | nullable enum `UPVOTE` \| `DOWNVOTE` \| `CONFIRM` \| `UNKNOWN` | What was cast; null on a retraction |
| `occurredAt` | long, `timestamp-millis` | UTC time of the change; a `java.time.Instant` in the generated class |
| `tallies` | record | The alert's counts **as of this change**, read from the write model in the same transaction |

Both enums carry `"default": "UNKNOWN"`. Avro enums are closed: without a default, adding a
vote type would make every reader built before it fail on the first message carrying the new
symbol. With one, old readers see `UNKNOWN` and keep going — which for a consumer that only
maintains counts is exactly right, since the tallies are already computed.

## Why one topic and not two

There were two, `vote.cast` and `vote.removed`. Kafka orders messages within a partition and
the key selects the partition, so one topic keyed by alert means everything said about an
alert arrives in the order it happened. Two topics have two sets of partitions and two
consumer containers polling them independently: a vote and an immediate retraction could be
applied in either order, and a consumer that writes the counts it is told would then hold
numbers that are wrong until the next vote on that alert.

This service's own consumer never had that problem, because it recomputes from the write
model rather than believing the message. nabat-app cannot — it does not own the votes.

The alternative was a watermark on each consumer: carry the event time, keep the last applied
one, ignore anything older. It compensates for disorder instead of preventing it, every
consumer has to implement it, and it is only as good as the producer's clock. One keyed topic
gives the same guarantee structurally.

The domain still has two events. `VoteCastEvent` and `VoteRemovedEvent` are different things
to say, and they stay different; `VoteChangedMessage` is the transport's shape, built at the
outbox boundary. `changeType` is there for consumers that care which happened — the
projection does not.

## Why the tallies are on the event

A consumer that keeps its own projection would otherwise have to apply a delta, which
double-counts the second time an event is delivered, or call back for the stats, which is a
synchronous hop onto a projection that is itself asynchronous. Absolute values make applying
the event idempotent: writing them twice is the same write.

`credibilityScore` is derived (`upvotes - downvotes + 2 × confirmations`) but travels
anyway, because `VoteCounts` is the single definition of that formula and a consumer
recomputing it would be a copy free to drift.

## Serialisation and the schema registry

A message is a magic byte, the four-byte id of the schema it was written with, then the Avro
body. The id is what a consumer resolves against the registry to know how to read the bytes;
neither side ships a copy of the schema, and nothing in the message names a field.

Serialisation happens when the outbox row is written, inside the transaction that writes the
vote, and the relay ships those bytes unchanged — so the wire format is fixed at commit time
rather than at send time. That means resolving the schema id would be an HTTP call inside a
database transaction, so `VoteEventAvroSerializer` registers the schema at startup through the
same client the serialiser uses; by the time a vote arrives, the id is cached.

Registering at startup also makes an incompatible change fail this service at boot rather than
at the first vote: the registry rejects the schema, and the application does not start.

| Setting | Value | Why |
|---|---|---|
| Subject | `vote.changed-value` | `TopicNameStrategy`, the default: one schema per topic |
| Compatibility | `BACKWARD` | A new schema must read old data — consumers replay this topic from the start to rebuild projections, so they meet old messages by design |
| `specific.avro.reader` | `true` | Deserialise into the generated class; without it every field access is an unchecked string lookup on a `GenericRecord` |

What `BACKWARD` allows: deleting a field, and adding one **with a default**. What it refuses:
adding a required field, or narrowing a type. A rename is deletion plus addition, which passes
the check and breaks the meaning — the registry enforces shape, never sense.

Tests point `nabat.schema-registry.url` at `mock://`, which Confluent's serialisers understand
as an in-memory registry shared per scope inside the JVM. That is why the suite needs no
registry container.

## Consumer Group

The voting service subscribes to `vote.changed` with the consumer group
`nabat-voting-group` (configurable via `spring.kafka.consumer.group-id`).

If additional microservices need to react to vote events (nabat-app already does, and a
notification service will), they use their own consumer group so that each service receives
every message independently.

## Partitioning

All messages for a given alert are published with `alertId` as the Kafka message key, so they
land in one partition and are delivered in the order they were written.  Nothing depends on
ordering *between* alerts.  This is the guarantee that made one topic worth more than two —
see above.

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
