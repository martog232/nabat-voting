-- One row per alert, holding nothing. Its only purpose is to be a row that a vote can take
-- a pessimistic lock on, so that two people voting on the same alert at the same time are
-- serialised against each other.
--
-- Why this is needed. castVote reads the tallies back from the write model inside its own
-- transaction and puts those absolute numbers on the outbox event. Under READ COMMITTED two
-- concurrent votes on one alert each read a total that does not include the other, because
-- neither has committed yet — so both events say "1 confirmation", the consumer applies an
-- absolute 1 twice, and an alert two people confirmed displays one confirmation. It never
-- corrects itself: recomputation only runs when the next event for that alert arrives.
--
-- Absolute tallies are what make redelivery idempotent, and that is worth keeping, so the
-- read and the write are made atomic per alert instead. The lock is held for the few
-- milliseconds of one vote and only ever contends with another vote on the *same* alert.
--
-- A separate table rather than alert_credibility: that one is a read model owned by the
-- Kafka consumer and its row does not exist until the first event has been processed, which
-- is exactly when the first vote needs the lock.
CREATE TABLE alert_vote_lock (
    alert_id VARCHAR(255) PRIMARY KEY
);

-- Backfill, so the row already exists for every alert that has ever been voted on and the
-- create-on-first-use path below is genuinely only hit by new alerts.
INSERT INTO alert_vote_lock (alert_id)
SELECT DISTINCT alert_id FROM votes;
