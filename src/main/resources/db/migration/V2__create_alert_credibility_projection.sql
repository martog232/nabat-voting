-- Read-model (CQRS projection) for alert credibility.
--
-- This table is a materialised view of the `votes` write-model. It is NOT the
-- source of truth: it is kept up to date asynchronously by the Kafka consumer
-- (see CredibilityProjectionUpdater) and can be fully rebuilt from `votes` at
-- any time. Reads (GET .../votes/stats) hit this table with a single primary-key
-- lookup instead of running COUNT(*) aggregations over `votes`.
CREATE TABLE alert_credibility (
    alert_id      VARCHAR(255) PRIMARY KEY,
    upvotes       INTEGER   NOT NULL DEFAULT 0,
    downvotes     INTEGER   NOT NULL DEFAULT 0,
    confirmations INTEGER   NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL
);
