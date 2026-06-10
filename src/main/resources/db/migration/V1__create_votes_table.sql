CREATE TABLE votes (
    id          UUID         PRIMARY KEY,
    alert_id    VARCHAR(255) NOT NULL,
    voter_id    VARCHAR(255) NOT NULL,
    vote_type   VARCHAR(20)  NOT NULL CHECK (vote_type IN ('UPVOTE', 'DOWNVOTE', 'CONFIRM')),
    cast_at     TIMESTAMP    NOT NULL,
    UNIQUE (alert_id, voter_id)
);

CREATE INDEX idx_votes_alert_id ON votes (alert_id);
CREATE INDEX idx_votes_voter_id ON votes (voter_id);
