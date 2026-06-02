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
           │ publish VoteCastEvent              │ consume VoteCastEvent
           ▼                                    │
┌───────────────────────┐            ┌──────────────────────────┐
│ KafkaVoteEventPublisher│            │ KafkaVoteEventConsumer   │
│   (infrastructure)    │            │    (infrastructure)      │
└──────────┬────────────┘            └────────────┬─────────────┘
           │                                      │
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
3. `KafkaVoteEventPublisher` serialises the event to JSON and sends it to the `vote.cast` Kafka
   topic using a `KafkaTemplate<String, VoteCastEvent>`.
4. `KafkaVoteEventConsumer` receives the message from the same topic and applies it to
   `CredibilityProjection`, updating the in-memory credibility score for the affected alert.

## Infrastructure Adapters

### KafkaVoteEventPublisher

Implements `VoteEventPublisher` (outbound port).  Uses `KafkaTemplate<String, VoteCastEvent>` with
a Jackson-based `JsonSerializer` to produce JSON messages.  The Kafka message key is the
`alertId`, ensuring that all votes for a given alert end up in the same partition (ordered
delivery per alert).

### KafkaVoteEventConsumer

Spring `@KafkaListener` that subscribes to the `vote.cast` topic.  Deserialises the JSON payload
into `VoteCastEvent` using `JsonDeserializer<VoteCastEvent>` and delegates to
`CredibilityProjection#apply(VoteCastEvent)`.

### InMemoryVoteRepository

Implements `VoteRepository` (outbound port) using a `CopyOnWriteArrayList`.  This is the default
implementation for local development.  A production deployment should replace it with a
database-backed adapter (e.g. Spring Data JPA or R2DBC).

## Testing Strategy

| Test | Type | What it validates |
|------|------|-------------------|
| `CastVoteServiceTest` | Unit | Service orchestration: vote persistence + event publication |
| `VoteKafkaIntegrationTest` | Integration (`@EmbeddedKafka`) | End-to-end Kafka flow: vote → event → projection |
| `NabatVotingApplicationTests` | Spring context | Application context loads successfully |
