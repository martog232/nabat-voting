# Architecture

## Overview

Nabat Voting is built around a **hexagonal (ports-and-adapters)** architecture to keep the domain
logic free of framework and infrastructure concerns.  All communication with external systems
(Kafka, database) is mediated through explicit port interfaces.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Application Core                        │
│                                                                 │
│  ┌──────────────────┐       ┌────────────────────────────────┐  │
│  │  CastVoteService  │──────▶│   VoteRepository (port/out)   │  │
│  │  (application     │       └────────────────────────────────┘  │
│  │   service)        │       ┌────────────────────────────────┐  │
│  │                   │──────▶│ VoteEventPublisher (port/out)  │  │
│  └──────────────────┘       └────────────────────────────────┘  │
│           ▲                                                      │
│  ┌────────┴─────────┐                                           │
│  │ CastVoteUseCase  │                                           │
│  │   (port/in)      │                                           │
│  └──────────────────┘                                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              CredibilityProjection                      │    │
│  │  (read model – updated by Kafka consumer)               │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
           │                                    ▲
           │ append to outbox                   │ consume VoteCastEvent
           │ (the vote's own transaction)       │
           ▼                                    │
┌────────────────────────┐           ┌──────────────────────────┐
│OutboxVoteEventPublisher│           │ KafkaVoteEventConsumer   │
│   (infrastructure)     │           │    (infrastructure)      │
└──────────┬─────────────┘           └────────────┬─────────────┘
           │ outbox_event                         │
           ▼                                      │
┌────────────────────────┐                        │
│      OutboxRelay       │                        │
│  (committed rows only) │                        │
└──────────┬─────────────┘                        │
           │           vote.cast topic            │
           └──────────────────────────────────────┘
                        Apache Kafka
```

## Domain Model

| Class | Description |
|-------|-------------|
| `Vote` | Aggregate root — a single vote cast by a voter on an alert |
| `VoteId` | UUID-based identifier for a vote |
| `AlertId` | String-based identifier of the alert being voted on |
| `VoterId` | String-based identifier of the voter |
| `VoteCastEvent` | Domain event emitted when a vote is successfully persisted |

## Event Flow

1. A caller invokes `CastVoteUseCase#castVote(CastVoteCommand)`.
2. `CastVoteService` creates a `Vote` aggregate, persists it via `VoteRepository`, and then calls
   `VoteEventPublisher#publish(VoteCastEvent)`.
3. `OutboxVoteEventPublisher` serialises the event and writes it to `outbox_event` — in the
   transaction that is writing the vote, so the two commit together or not at all.
4. `OutboxRelay` polls for committed rows, sends them to the `vote.cast` topic, and marks them
   published.
5. `KafkaVoteEventConsumer` receives the message and drives `CredibilityProjectionUpdater`, which
   recomputes the affected alert's counts from the `votes` write model.

### Why the event goes through a table

Steps 2 and 3 used to be one step: publish straight to Kafka from inside the transaction. That is
a dual write, and it fails in both directions. A crash between commit and send loses the event.
A send that overtakes its own commit reaches the consumer first, which then recomputes the
projection from a write model that does not hold the vote yet and stores zeros — permanently,
because recomputation only runs again on the next event for that alert. Both were observed while
writing nabat-app's `VotingServiceIntegrationTest`.

Publishing *after* commit narrows that window instead of closing it, and adds a third failure:
a send that never happens because the process died first. Only a row that commits with the vote
removes the question.

Delivery is at-least-once — the send and the `published_at` update are not atomic either — which
is safe here because recomputation is idempotent.

## Infrastructure Adapters

### OutboxVoteEventPublisher

Implements `VoteEventPublisher` (outbound port) by appending to `outbox_event` rather than
sending anything.  Serialisation happens here, with the same `JsonMapper` the consumer reads
with, so the bytes stored are the bytes sent.  The Kafka message key is the `alertId`, ensuring
that all votes for a given alert end up in the same partition (ordered delivery per alert).

### OutboxRelay

Scheduled (`nabat.outbox.poll-interval`, 1s) drain of committed rows, oldest first, claimed with
`SELECT ... FOR UPDATE SKIP LOCKED` so replicas do not send the same row twice.  A failed send
stops its batch rather than skipping past it, to keep per-alert order on the topic; the row stays
pending and its `attempts` and `last_error` columns make a row failing forever visible.  A second
schedule drops rows published longer ago than `nabat.outbox.retention`.

### KafkaVoteEventConsumer

Spring `@KafkaListener` that subscribes to the `vote.cast` and `vote.removed` topics.
Deserialises the JSON payload with `JacksonJsonDeserializer` and delegates to
`CredibilityProjectionUpdater`, which recomputes rather than applying deltas — so a redelivered
event produces the same row.

### PostgresVoteRepositoryAdapter

Implements `VoteRepository` (outbound port) using Spring Data JPA (via `VoteJpaEntity` /
`VoteJpaRepository`).  This is the production adapter and is annotated `@Primary` so it takes
precedence over any in-memory alternative.

## Testing Strategy

| Test | Type | What it validates |
|------|------|-------------------|
| `CastVoteServiceTest` | Unit | Service orchestration: vote persistence + event publication |
| `VoteKafkaIntegrationTest` | Integration (`@EmbeddedKafka`) | End-to-end Kafka flow: vote → outbox → event → projection |
| `OutboxIntegrationTest` | Integration (`@EmbeddedKafka`) | The event shares the vote's transaction: it is visible inside it, gone when it rolls back, and sent by the relay once it commits |
| `NabatVotingApplicationTests` | Spring context | Application context loads successfully |
