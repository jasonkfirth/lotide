CREATE TABLE person_follow (
    target BIGINT REFERENCES person,
    follower BIGINT REFERENCES person,

    PRIMARY KEY (target, follower)
);
BEGIN;
    ALTER TABLE person_follow ADD COLUMN local BOOLEAN;
    UPDATE person_follow SET local = (SELECT local FROM person WHERE id = person_follow.follower);
    ALTER TABLE person_follow ALTER COLUMN local SET NOT NULL;

    ALTER TABLE person_follow ADD COLUMN ap_id TEXT;
    ALTER TABLE person_follow ADD COLUMN accepted BOOLEAN;
    UPDATE person_follow SET accepted = TRUE;
    ALTER TABLE person_follow ALTER COLUMN accepted SET NOT NULL;
COMMIT;

CREATE INDEX IF NOT EXISTS person_follow_target_idx ON person_follow (target);
CREATE INDEX IF NOT EXISTS person_follow_follower_idx ON person_follow (follower);
CREATE INDEX IF NOT EXISTS person_follow_local_accepted_idx ON person_follow (follower, target) WHERE local AND accepted;