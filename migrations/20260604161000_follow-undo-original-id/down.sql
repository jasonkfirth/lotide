ALTER TABLE local_user_follow_undo
    DROP COLUMN follow_ap_id;

ALTER TABLE local_community_follow_undo
    DROP COLUMN follow_ap_id;
