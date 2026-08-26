-- Transactional outbox for vote events.
--
-- The publish used to happen straight to Kafka from inside the transaction that writes the
-- vote: a dual write, with two ways to lose. A crash between commit and send drops the event
-- entirely. And a send that *beats* its own commit lets the consumer recompute the
-- credibility projection from a write model that does not hold the vote yet — it then stores
-- zeros, permanently, because recomputation only runs again on the next event for that alert.
-- Both were observed while writing nabat-app's VotingServiceIntegrationTest.
--
-- A row here is written in the same transaction as the vote, so the two commit together or
-- not at all, and the relay reads only committed rows. That is what makes the second failure
-- impossible rather than merely unlikely: publishing after commit narrows the window, it does
-- not close it.
CREATE TABLE outbox_event (
    id            UUID         PRIMARY KEY,
    topic         VARCHAR(255) NOT NULL,
    event_type    VARCHAR(255) NOT NULL,
    -- Kafka message key: the alert id, so all events for one alert keep their order.
    partition_key VARCHAR(255) NOT NULL,
    -- The exact bytes to send. Serialised once, here, rather than reconstructed at send
    -- time: a payload that serialises but no longer deserialises would otherwise turn every
    -- pending row into a permanent failure, discoverable only after a crash.
    payload       TEXT         NOT NULL,
    occurred_at   TIMESTAMP    NOT NULL,
    published_at  TIMESTAMP,
    attempts      INTEGER      NOT NULL DEFAULT 0,
    last_error    TEXT
);

-- The relay's only query: oldest unpublished first. Partial, because once the backlog is
-- drained the published rows are all of the table and none of the interest.
CREATE INDEX idx_outbox_pending ON outbox_event (occurred_at) WHERE published_at IS NULL;

-- The purge's query: published long enough ago to drop.
CREATE INDEX idx_outbox_published_at ON outbox_event (published_at) WHERE published_at IS NOT NULL;
