BEGIN;
	DROP INDEX local_collection_target_follow_undo_target_idx;
	DROP TABLE local_collection_target_follow_undo;

	DROP INDEX collection_target_follow_local_accepted_idx;
	DROP INDEX collection_target_follow_follower_idx;
	DROP TABLE collection_target_follow;

	DROP INDEX collection_target_target_kind_idx;
	DROP INDEX collection_target_owner_actor_idx;
	DROP TABLE collection_target;
COMMIT;
