ALTER TABLE local_community_follow_undo
    ADD COLUMN follow_ap_id TEXT;

ALTER TABLE local_user_follow_undo
    ADD COLUMN follow_ap_id TEXT;
