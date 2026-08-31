-- The outbox payload is Avro now, not JSON.
--
-- Confluent's wire format is a magic byte, the id of the schema the record was written with,
-- then the Avro body — binary, not text. The column has to hold bytes for the same reason it
-- held text before: what is stored is exactly what goes on the topic, so that a schema change
-- can never turn a pending row into one that cannot be sent.
--
-- Existing rows are JSON written for the previous format. They are all published — the relay
-- drains within a second and the purge only keeps them as a record of what was sent — so the
-- conversion below is about keeping them readable, not about sending them. If a pending row
-- did survive this migration it would be sent as JSON bytes and rejected by a consumer
-- expecting Avro, which is loud rather than silent, and one vote's counts behind.
ALTER TABLE outbox_event
    ALTER COLUMN payload TYPE BYTEA USING convert_to(payload, 'UTF8');
