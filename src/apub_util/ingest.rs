use super::{ExtendedPostlike, FollowLike, KnownObject, Verified};
use crate::hyper;
use crate::types::{
    CollectionTargetLocalID, CommentLocalID, CommunityLocalID, NotificationID, PollOptionLocalID,
    PostLocalID, ThingLocalRef, UserLocalID,
};
use activitystreams::prelude::*;
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::sync::Arc;

const REMOTE_COMMUNITY_TRACKING_SQL: &str = "\
SELECT deleted, EXISTS(\
    SELECT 1 FROM community_follow \
    WHERE community_follow.community=community.id \
    AND community_follow.local \
    AND community_follow.accepted\
) FROM community WHERE id=$1";
const DELETE_LOCAL_COMMUNITY_FOLLOWS_FOR_UNTRACKED_SQL: &str =
    "DELETE FROM community_follow WHERE community=$1 AND local RETURNING follower, ap_id";
const DELETE_EMPTY_UNTRACKED_REMOTE_COMMUNITY_SQL: &str = "\
DELETE FROM community \
WHERE id=$1 \
AND NOT local \
AND NOT deleted \
AND NOT EXISTS (\
    SELECT 1 FROM community_follow \
    WHERE community_follow.community=community.id\
) \
AND NOT EXISTS (\
    SELECT 1 FROM post \
    WHERE post.community=community.id\
)";
const UPSERT_ACTOR_TARGET_PROFILE_SQL: &str = "\
INSERT INTO actor_target_profile \
(actor_ap_id, target, family, actor_kind, source, confidence, has_inbox, has_outbox, has_followers, has_featured, evidence) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
ON CONFLICT (actor_ap_id) DO UPDATE SET \
target=(CASE WHEN actor_target_profile.confidence > EXCLUDED.confidence THEN actor_target_profile.target ELSE EXCLUDED.target END), \
family=(CASE WHEN actor_target_profile.confidence > EXCLUDED.confidence THEN actor_target_profile.family ELSE EXCLUDED.family END), \
actor_kind=(CASE WHEN actor_target_profile.confidence > EXCLUDED.confidence THEN actor_target_profile.actor_kind ELSE EXCLUDED.actor_kind END), \
source=(CASE WHEN actor_target_profile.confidence > EXCLUDED.confidence THEN actor_target_profile.source ELSE EXCLUDED.source END), \
confidence=GREATEST(actor_target_profile.confidence, EXCLUDED.confidence), \
has_inbox=EXCLUDED.has_inbox, \
has_outbox=EXCLUDED.has_outbox, \
has_followers=EXCLUDED.has_followers, \
has_featured=EXCLUDED.has_featured, \
evidence=actor_target_profile.evidence || EXCLUDED.evidence, \
updated_at=current_timestamp";
const UPSERT_ACTOR_TARGET_OBJECT_OBSERVATION_SQL: &str = "\
INSERT INTO actor_target_profile \
(actor_ap_id, target, family, actor_kind, source, confidence, evidence, observed_object_types) \
VALUES ($1, 'UnknownGroup', 'CollectionChannel', 'Group', 'observation', 20, $3, ARRAY[$2]::TEXT[]) \
ON CONFLICT (actor_ap_id) DO UPDATE SET \
observed_object_types=(CASE WHEN $2 = ANY(actor_target_profile.observed_object_types) THEN actor_target_profile.observed_object_types ELSE array_append(actor_target_profile.observed_object_types, $2) END), \
evidence=actor_target_profile.evidence || EXCLUDED.evidence, \
updated_at=current_timestamp";
const POSTLIKE_AUTHOR_IS_COMMUNITY_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM community WHERE id=$1 AND ap_id=$2)";
const UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL: &str = "\
INSERT INTO person \
(username, local, created_local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, is_bot) \
SELECT name, FALSE, current_timestamp, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, TRUE \
FROM community \
WHERE id=$1 \
AND NOT local \
AND NOT deleted \
AND ap_id=$2 \
ON CONFLICT (ap_id) DO UPDATE SET \
username=EXCLUDED.username, \
ap_inbox=EXCLUDED.ap_inbox, \
ap_shared_inbox=EXCLUDED.ap_shared_inbox, \
public_key=EXCLUDED.public_key, \
public_key_sigalg=EXCLUDED.public_key_sigalg, \
description_html=EXCLUDED.description_html, \
is_bot=TRUE \
RETURNING id";
const MARK_LOCAL_POST_LIKE_POSTED_SQL: &str = "\
UPDATE post_like \
SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), \
federation_received_at=COALESCE(federation_received_at, current_timestamp), \
federation_posted_at=COALESCE(federation_posted_at, current_timestamp) \
WHERE post=$1 AND person=$2 AND local";
const MARK_LOCAL_REPLY_LIKE_POSTED_SQL: &str = "\
UPDATE reply_like \
SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), \
federation_received_at=COALESCE(federation_received_at, current_timestamp), \
federation_posted_at=COALESCE(federation_posted_at, current_timestamp) \
WHERE reply=$1 AND person=$2 AND local";

async fn mark_local_follow_response(
    db: &tokio_postgres::Client,
    actor_ap_id: &str,
    object_id: &activitystreams::iri_string::types::IriString,
    accepted: bool,
    ctx: &Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let community_local_id: Option<CommunityLocalID> = db
        .query_opt("SELECT id FROM community WHERE ap_id=$1", &[&actor_ap_id])
        .await?
        .map(|row| CommunityLocalID(row.get(0)));

    let user_local_id: Option<UserLocalID> = db
        .query_opt(
            "SELECT id FROM person WHERE local AND ap_id=$1",
            &[&actor_ap_id],
        )
        .await?
        .map(|row| UserLocalID(row.get(0)));

    let Some(remaining) = crate::apub_util::try_strip_host(object_id, &ctx.host_url_apub) else {
        return Ok(());
    };

    let obj_ref = super::LocalObjectRef::try_from_path(remaining);
    match obj_ref {
        Some(
            super::LocalObjectRef::UserFollow(target_user_id, follower_local_id)
            | super::LocalObjectRef::UserFollowJoin(target_user_id, follower_local_id),
        ) => {
            if user_local_id == Some(target_user_id) {
                db.execute(
                    "UPDATE person_follow SET accepted=$3, federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE target=$1 AND follower=$2",
                    &[&target_user_id, &follower_local_id, &accepted],
                )
                .await?;
            }
        }
        Some(
            super::LocalObjectRef::CommunityFollow(community_id, follower_local_id)
            | super::LocalObjectRef::CommunityFollowJoin(community_id, follower_local_id),
        ) => {
            if community_local_id == Some(community_id) {
                db.execute(
                    "UPDATE community_follow SET accepted=$3, federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE community=$1 AND follower=$2",
                    &[&community_id, &follower_local_id, &accepted],
                )
                .await?;
            }
        }
        Some(super::LocalObjectRef::CollectionTargetFollow(
            collection_target_id,
            follower_local_id,
        )) => {
            db.execute(
                "UPDATE collection_target_follow SET accepted=$4, federation_sent_at=COALESCE(federation_sent_at, current_timestamp), federation_received_at=COALESCE(federation_received_at, current_timestamp) WHERE collection_target=$1 AND follower=$2 AND EXISTS (SELECT 1 FROM collection_target WHERE collection_target.id=$1 AND collection_target.owner_ap_id=$3)",
                &[&collection_target_id, &follower_local_id, &actor_ap_id, &accepted],
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
}

struct RemoteCommunityActor<'a> {
    ap_id: &'a activitystreams::iri_string::types::IriString,
    name: &'a str,
    description_html: Option<&'a str>,
    inbox: &'a str,
    outbox: Option<&'a activitystreams::iri_string::types::IriString>,
    followers: Option<&'a str>,
    shared_inbox: Option<&'a str>,
    public_key: Option<&'a [u8]>,
    public_key_sigalg: Option<&'a str>,
    featured: Option<url::Url>,
}

#[derive(Debug, Clone)]
pub enum FoundFrom {
    Announce {
        url: activitystreams::iri_string::types::IriString,
        community_local_id: CommunityLocalID,
        community_is_local: bool,
        allow_untracked_remote_community: bool,
    },
    CommunityOutbox {
        community_local_id: CommunityLocalID,
        community_is_local: bool,
        preview: bool,
    },
    ExplicitLookup,
    Refresh,
    Other,
}

impl FoundFrom {
    pub fn approved_ap_id(&self) -> Option<&str> {
        match self {
            FoundFrom::Announce { url, .. } => Some(url.as_str()),
            _ => None,
        }
    }

    pub fn approves_post(&self) -> bool {
        matches!(
            self,
            FoundFrom::Announce { .. } | FoundFrom::CommunityOutbox { .. }
        )
    }

    fn confirms_local_content(&self) -> bool {
        self.approves_post() || matches!(self, FoundFrom::Refresh)
    }

    fn community(&self) -> Option<(CommunityLocalID, bool)> {
        match self {
            FoundFrom::Announce {
                community_local_id,
                community_is_local,
                ..
            }
            | FoundFrom::CommunityOutbox {
                community_local_id,
                community_is_local,
                ..
            } => Some((*community_local_id, *community_is_local)),
            _ => None,
        }
    }

    fn allows_untracked_remote_community(&self) -> bool {
        match self {
            FoundFrom::Announce {
                allow_untracked_remote_community,
                ..
            } => *allow_untracked_remote_community,
            FoundFrom::CommunityOutbox { preview, .. } => *preview,
            FoundFrom::ExplicitLookup => true,
            _ => false,
        }
    }

    pub fn keeps_untracked_remote_group(&self) -> bool {
        matches!(self, FoundFrom::ExplicitLookup)
    }
}

fn reply_parent_fetch_found_from(found_from: &FoundFrom) -> FoundFrom {
    if found_from.approves_post() {
        found_from.clone()
    } else {
        FoundFrom::Refresh
    }
}

async fn get_or_fetch_postlike_author_local_id(
    author_ap_id: Option<&activitystreams::iri_string::types::IriString>,
    community_local_id: Option<CommunityLocalID>,
    db: &tokio_postgres::Client,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<UserLocalID>, crate::Error> {
    let Some(author_ap_id) = author_ap_id else {
        return Ok(None);
    };

    if let Some(community_local_id) = community_local_id {
        let author_is_community = db
            .query_one(
                POSTLIKE_AUTHOR_IS_COMMUNITY_SQL,
                &[&community_local_id, &author_ap_id.as_str()],
            )
            .await?
            .get(0);

        if author_is_community {
            return db
                .query_opt(
                    UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL,
                    &[&community_local_id, &author_ap_id.as_str()],
                )
                .await
                .map(|row| row.map(|row| UserLocalID(row.get(0))))
                .map_err(Into::into);
        }
    }

    super::get_or_fetch_user_local_id(author_ap_id, db, ctx)
        .await
        .map(Some)
}

async fn mark_local_post_seen_from_remote(
    db: &tokio_postgres::Client,
    post_id: PostLocalID,
    found_from: &FoundFrom,
) -> Result<(), crate::Error> {
    let Some((community_id, community_is_local)) = found_from.community() else {
        return Ok(());
    };

    if community_is_local {
        return Ok(());
    }

    db.execute(
        "UPDATE post SET approved=TRUE, approved_ap_id=COALESCE($1::TEXT, approved_ap_id), rejected=FALSE, rejected_ap_id=NULL WHERE id=$2 AND community=$3 AND local",
        &[&found_from.approved_ap_id(), &post_id, &community_id],
    )
    .await?;

    Ok(())
}

async fn mark_local_comment_seen_from_remote(
    db: &tokio_postgres::Client,
    comment_id: CommentLocalID,
    found_from: &FoundFrom,
) -> Result<(), crate::Error> {
    if !found_from.confirms_local_content() {
        return Ok(());
    }

    db.execute(
        "UPDATE reply SET federation_posted_at=COALESCE(federation_posted_at, current_timestamp), federation_posted_ap_id=COALESCE($1::TEXT, federation_posted_ap_id) WHERE id=$2 AND local AND EXISTS (SELECT 1 FROM post INNER JOIN community ON community.id=post.community WHERE post.id=reply.post AND NOT community.local)",
        &[&found_from.approved_ap_id(), &comment_id],
    )
    .await?;

    Ok(())
}

async fn upsert_actor_target_profile(
    db: &tokio_postgres::Client,
    actor_ap_id: &str,
    profile: &super::target::TargetProfile,
) -> Result<(), crate::Error> {
    /*
        Actor target profiles are a memory of what lotide has learned about a
        remote actor. A later classifier result can replace an early heuristic,
        while observed evidence is kept so future code can see why the actor
        was treated as a group, relay, blog, or profile.
    */
    let evidence = profile.evidence_json();
    let target = profile.target.as_str();
    let family = profile.family.as_str();
    let actor_kind = profile.actor_kind.as_str();
    let source = profile.source();
    let confidence = profile.confidence();

    db.execute(
        UPSERT_ACTOR_TARGET_PROFILE_SQL,
        &[
            &actor_ap_id,
            &target,
            &family,
            &actor_kind,
            &source,
            &confidence,
            &profile.has_inbox,
            &profile.has_outbox,
            &profile.has_followers,
            &profile.has_featured,
            &evidence,
        ],
    )
    .await?;

    Ok(())
}

async fn record_community_observed_object_type(
    community_local_id: CommunityLocalID,
    object_type: &str,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;
    let actor_ap_id = db
        .query_opt(
            "SELECT ap_id FROM community WHERE id=$1 AND NOT local",
            &[&community_local_id],
        )
        .await?
        .and_then(|row| row.get::<_, Option<String>>(0));

    if let Some(actor_ap_id) = actor_ap_id {
        let evidence = serde_json::json!({
            "content": {
                "last_observed_object_type": object_type
            }
        });

        db.execute(
            UPSERT_ACTOR_TARGET_OBJECT_OBSERVATION_SQL,
            &[&actor_ap_id, &object_type, &evidence],
        )
        .await?;
    }

    Ok(())
}

fn embedded_activity_object_is_trusted(
    activity_id: &activitystreams::iri_string::types::IriString,
    object_id: &activitystreams::iri_string::types::IriString,
) -> bool {
    let same_origin = match (
        activity_id.as_str().parse::<url::Url>(),
        object_id.as_str().parse::<url::Url>(),
    ) {
        (Ok(activity_url), Ok(object_url)) => match (activity_url.host(), object_url.host()) {
            (Some(activity_host), Some(object_host)) => {
                activity_host == object_host && activity_url.port() == object_url.port()
            }
            _ => false,
        },
        _ => false,
    };

    same_origin || activity_id.as_str().starts_with(object_id.as_str())
}

pub enum IngestResult {
    Actor(super::ActorLocalInfo),
    Post(PostIngestResult),
    Other(ThingLocalRef),
}

impl IngestResult {
    pub fn into_ref(self) -> ThingLocalRef {
        match self {
            IngestResult::Actor(info) => info.as_ref(),
            IngestResult::Post(info) => ThingLocalRef::Post(info.id),
            IngestResult::Other(x) => x,
        }
    }
}

pub struct PostIngestResult {
    pub id: PostLocalID,
    pub poll: Option<MaybeElided<crate::PollInfoOwned>>,
}

pub struct MaybeElided<T>(pub Option<T>);

impl<T> MaybeElided<T> {
    pub const ELIDED: Self = MaybeElided(None);
}

impl<T> From<T> for MaybeElided<T> {
    fn from(src: T) -> MaybeElided<T> {
        MaybeElided(Some(src))
    }
}

const UTC_OFFSET: chrono::offset::FixedOffset = match chrono::offset::FixedOffset::east_opt(0) {
    Some(value) => value,
    None => unreachable!(),
};

/*
    ActivityPub objects can deserialize into very large enum variants. The
    public ingest entry point returns a boxed future so task workers do not
    carry that state on their own stack. Splitting this dispatcher further
    would be a wider ingest refactor, so keep this lint scoped here.
*/
#[allow(clippy::large_stack_frames)]
pub fn ingest_object(
    object: Box<Verified<KnownObject>>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
    for_inbox: bool,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<IngestResult>, crate::Error>> + Send>> {
    Box::pin(async move {
        let mut db = ctx.db_pool.get().await?;

        if let Some(id) = object.id() {
            // Detect local objects and skip ingestion (#217)
            if let Some(obj_ref) = super::LocalObjectRef::try_from_uri(id, &ctx.host_url_apub) {
                return match obj_ref {
                    super::LocalObjectRef::User(user_id) => {
                        let person = match &object.0 {
                            KnownObject::Person(person) => person,
                            _ => unreachable!(),
                        };

                        let public_key = person
                            .ext_one
                            .public_key
                            .as_ref()
                            .map(|key| key.public_key_pem.as_bytes());
                        let public_key_sigalg = person
                            .ext_one
                            .public_key
                            .as_ref()
                            .and_then(|key| key.signature_algorithm.as_deref());

                        Ok(Some(IngestResult::Actor(super::ActorLocalInfo::User {
                            id: user_id,
                            public_key: public_key.map(|key| super::PubKeyInfo {
                                algorithm: super::get_message_digest(public_key_sigalg),
                                key: key.to_owned(),
                            }),
                            remote_url: super::url_from_ap_id(id)?,
                        })))
                    }
                    super::LocalObjectRef::Community(community_id) => {
                        let group = match &object.0 {
                            KnownObject::Group(group) => group,
                            _ => unreachable!(),
                        };

                        let public_key = group
                            .ext_one
                            .public_key
                            .as_ref()
                            .map(|key| key.public_key_pem.as_bytes());
                        let public_key_sigalg = group
                            .ext_one
                            .public_key
                            .as_ref()
                            .and_then(|key| key.signature_algorithm.as_deref());

                        Ok(Some(IngestResult::Actor(
                            super::ActorLocalInfo::Community {
                                id: community_id,
                                public_key: public_key.map(|key| super::PubKeyInfo {
                                    algorithm: super::get_message_digest(public_key_sigalg),
                                    key: key.to_owned(),
                                }),
                                ap_outbox: group
                                    .outbox_unchecked()
                                    .map(super::url_from_ap_id)
                                    .transpose()?,
                            },
                        )))
                    }
                    super::LocalObjectRef::Post(post_id) => {
                        mark_local_post_seen_from_remote(&db, post_id, &found_from).await?;

                        let has_poll = matches!(&object.0, KnownObject::Question(_));

                        Ok(Some(IngestResult::Post(PostIngestResult {
                            id: post_id,
                            poll: if has_poll {
                                Some(MaybeElided::ELIDED)
                            } else {
                                None
                            },
                        })))
                    }
                    super::LocalObjectRef::Comment(comment_id) => {
                        mark_local_comment_seen_from_remote(&db, comment_id, &found_from).await?;

                        Ok(Some(IngestResult::Other(ThingLocalRef::Comment(
                            comment_id,
                        ))))
                    }
                    _ => Ok(None),
                };
            }

            // check blocked objects
            let row = db
                .query_opt(
                    "SELECT 1 FROM blocked_ap_id WHERE ap_id=$1",
                    &[&id.as_str()],
                )
                .await?;
            if row.is_some() {
                return Err(crate::Error::InternalStrStatic("Blocked by admin"));
            }
        }

        if let Some(target_profile) = super::target::classify_known_object(&object) {
            let target_spec = super::target::target_spec(target_profile.target);
            let supported_operation_count = super::target::FEDERATION_OPERATIONS
                .iter()
                .filter(|operation| {
                    target_profile.support(**operation)
                        != super::target::OperationSupport::Unsupported
                })
                .count();

            log::debug!(
                "ActivityPub actor target profile: target={:?} family={:?} kind={:?} inbox={} outbox={} followers={} featured={} object_types={} activity_types={} supported_operations={}",
                target_profile.target,
                target_profile.family,
                target_profile.actor_kind,
                target_profile.has_inbox,
                target_profile.has_outbox,
                target_profile.has_followers,
                target_profile.has_featured,
                target_spec.object_types.len(),
                target_spec.activity_types.len(),
                supported_operation_count,
            );

            if let Some(actor_ap_id) = object.id() {
                upsert_actor_target_profile(&db, actor_ap_id.as_str(), &target_profile).await?;
            }
        }

        match (*object).into_inner() {
            KnownObject::Accept(activity) => {
                let actor_ap_id = followlike_id(activity.actor_unchecked())
                    .ok_or(crate::Error::InternalStrStatic("Missing actor for Accept"))?;

                /*
                    Accept activities often embed the original Follow object
                    instead of naming it with a plain string. The followlike_id
                    helper accepts both shapes, then the local object reference
                    decides which follow table should be marked accepted.
                */
                if let Some(activity_id) = activity.id_unchecked() {
                    if !crate::apub_util::is_contained(activity_id, &actor_ap_id) {
                        log::warn!(
                            "Accept activity ID {activity_id} does not share actor origin {actor_ap_id}"
                        );
                    }
                }

                if let Some(object_id) = followlike_id(activity.object()) {
                    mark_local_follow_response(&db, actor_ap_id.as_str(), &object_id, true, &ctx)
                        .await?;
                }

                Ok(None)
            }
            KnownObject::Reject(activity) => {
                let actor_ap_id = followlike_id(activity.actor_unchecked())
                    .ok_or(crate::Error::InternalStrStatic("Missing actor for Reject"))?;

                /*
                    A remote Reject is still a useful delivery response. Record
                    it as received and not accepted so the follow status stops
                    looking like an unprocessed queue problem.
                */
                if let Some(activity_id) = activity.id_unchecked() {
                    if !crate::apub_util::is_contained(activity_id, &actor_ap_id) {
                        log::warn!(
                            "Reject activity ID {activity_id} does not share actor origin {actor_ap_id}"
                        );
                    }
                }

                if let Some(object_id) = followlike_id(activity.object()) {
                    mark_local_follow_response(&db, actor_ap_id.as_str(), &object_id, false, &ctx)
                        .await?;
                }

                Ok(None)
            }
            KnownObject::Add(activity) => {
                let (actor, object, _origin, target, activity) = activity.into_parts();

                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

                let target = target
                    .as_ref()
                    .and_then(|x| x.as_single_id())
                    .ok_or(crate::Error::InternalStrStatic("Missing target for Add"))?;

                let community_ap_id = actor
                    .as_single_id()
                    .ok_or(crate::Error::InternalStrStatic("Missing actor for Add"))?;

                let res = db
                    .query_opt(
                        "SELECT id, local, ap_outbox FROM community WHERE ap_id=$1",
                        &[&community_ap_id.as_str()],
                    )
                    .await?;
                let community_local_info: Option<(CommunityLocalID, bool, Option<&str>)> = res
                    .as_ref()
                    .map(|row| (CommunityLocalID(row.get(0)), row.get(1), row.get(2)));

                if let Some((community_local_id, community_is_local, ap_outbox)) =
                    community_local_info
                {
                    let target_is_outbox = if let Some(ap_outbox) = ap_outbox {
                        ap_outbox == target.as_str()
                    } else {
                        let actor =
                            crate::apub_util::fetch_actor(community_ap_id, ctx.clone()).await?;

                        if let crate::apub_util::ActorLocalInfo::Community { ap_outbox, .. } = actor
                        {
                            if let Some(ap_outbox) = ap_outbox {
                                ap_outbox.as_str() == target.as_str()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };

                    if target_is_outbox {
                        crate::apub_util::require_containment(activity_id, community_ap_id)?;
                        crate::apub_util::require_containment(target, community_ap_id)?;

                        let object_id = object.as_single_id();

                        if let Some(object_id) = object_id {
                            if let Some(remaining) =
                                crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                            {
                                if let Some(local_object) =
                                    crate::apub_util::LocalObjectRef::try_from_path(remaining)
                                {
                                    let found_from = FoundFrom::Announce {
                                        url: activity_id.clone(),
                                        community_local_id,
                                        community_is_local,
                                        allow_untracked_remote_community: found_from
                                            .allows_untracked_remote_community(),
                                    };

                                    match local_object {
                                        crate::apub_util::LocalObjectRef::Post(local_post_id) => {
                                            mark_local_post_seen_from_remote(
                                                &db,
                                                local_post_id,
                                                &found_from,
                                            )
                                            .await?;
                                        }
                                        crate::apub_util::LocalObjectRef::Comment(
                                            local_comment_id,
                                        ) => {
                                            mark_local_comment_seen_from_remote(
                                                &db,
                                                local_comment_id,
                                                &found_from,
                                            )
                                            .await?;
                                        }
                                        _ => {}
                                    }
                                }
                            } else {
                                let obj = crate::apub_util::fetch_or_verify(
                                    community_ap_id,
                                    object.one().unwrap(),
                                    &ctx,
                                    for_inbox,
                                )
                                .await?;

                                ingest_object_boxed(
                                    obj,
                                    FoundFrom::Announce {
                                        url: activity_id.clone(),
                                        community_local_id,
                                        community_is_local,
                                        allow_untracked_remote_community: found_from
                                            .allows_untracked_remote_community(),
                                    },
                                    ctx,
                                    for_inbox,
                                )
                                .await?;
                            }
                        } else if let Some(embedded_object) = object.one() {
                            let obj = crate::apub_util::fetch_or_verify(
                                community_ap_id,
                                embedded_object,
                                &ctx,
                                for_inbox,
                            )
                            .await?;

                            ingest_object_boxed(
                                obj,
                                FoundFrom::Announce {
                                    url: activity_id.clone(),
                                    community_local_id,
                                    community_is_local,
                                    allow_untracked_remote_community: found_from
                                        .allows_untracked_remote_community(),
                                },
                                ctx,
                                for_inbox,
                            )
                            .await?;
                        }
                    }
                }

                Ok(None)
            }
            KnownObject::Announce(activity) => {
                let (actor, object, _target, activity) = activity.into_parts();

                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

                let community_ap_id = actor.as_single_id().ok_or(
                    crate::Error::InternalStrStatic("Missing actor for Announce"),
                )?;

                let community_local_info = db
                    .query_opt(
                        "SELECT id, local FROM community WHERE ap_id=$1",
                        &[&community_ap_id.as_str()],
                    )
                    .await?
                    .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

                if let Some((community_local_id, community_is_local)) = community_local_info {
                    crate::apub_util::require_containment(activity_id, community_ap_id)?;

                    let object_id = object.as_single_id();

                    if let Some(object_id) = object_id {
                        if let Some(remaining) =
                            crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                        {
                            if let Some(local_object) =
                                crate::apub_util::LocalObjectRef::try_from_path(remaining)
                            {
                                let found_from = FoundFrom::Announce {
                                    url: activity_id.clone(),
                                    community_local_id,
                                    community_is_local,
                                    allow_untracked_remote_community: found_from
                                        .allows_untracked_remote_community(),
                                };

                                match local_object {
                                    crate::apub_util::LocalObjectRef::Post(local_post_id) => {
                                        mark_local_post_seen_from_remote(
                                            &db,
                                            local_post_id,
                                            &found_from,
                                        )
                                        .await?;
                                    }
                                    crate::apub_util::LocalObjectRef::Comment(local_comment_id) => {
                                        mark_local_comment_seen_from_remote(
                                            &db,
                                            local_comment_id,
                                            &found_from,
                                        )
                                        .await?;
                                    }
                                    crate::apub_util::LocalObjectRef::PostLike(_, _)
                                    | crate::apub_util::LocalObjectRef::CommentLike(_, _) => {
                                        mark_local_like_posted(&db, local_object).await?;
                                    }
                                    _ => {}
                                }
                            }
                        } else {
                            let obj = crate::apub_util::fetch_or_verify(
                                community_ap_id,
                                object.one().unwrap(),
                                &ctx,
                                for_inbox,
                            )
                            .await?;

                            ingest_object_boxed(
                                obj,
                                FoundFrom::Announce {
                                    url: activity_id.clone(),
                                    community_local_id,
                                    community_is_local,
                                    allow_untracked_remote_community: found_from
                                        .allows_untracked_remote_community(),
                                },
                                ctx,
                                for_inbox,
                            )
                            .await?;
                        }
                    } else if let Some(local_object_id) =
                        local_announced_object_id(&object, &ctx.host_url_apub)
                    {
                        let found_from = FoundFrom::Announce {
                            url: activity_id.clone(),
                            community_local_id,
                            community_is_local,
                            allow_untracked_remote_community: found_from
                                .allows_untracked_remote_community(),
                        };

                        mark_local_announced_object_seen(&db, &local_object_id, &found_from, &ctx)
                            .await?;
                    } else if let Some(embedded_object) = object.one() {
                        let obj = crate::apub_util::fetch_or_verify(
                            community_ap_id,
                            embedded_object,
                            &ctx,
                            for_inbox,
                        )
                        .await?;

                        ingest_object_boxed(
                            obj,
                            FoundFrom::Announce {
                                url: activity_id.clone(),
                                community_local_id,
                                community_is_local,
                                allow_untracked_remote_community: found_from
                                    .allows_untracked_remote_community(),
                            },
                            ctx,
                            for_inbox,
                        )
                        .await?;
                    }
                }
                Ok(None)
            }
            KnownObject::Article(obj) => {
                ingest_postlike(Verified(KnownObject::Article(obj)), found_from, ctx).await
            }
            KnownObject::Audio(obj) => {
                ingest_postlike(Verified(KnownObject::Audio(obj)), found_from, ctx).await
            }
            KnownObject::Create(activity) => {
                ingest_create(Verified(activity), found_from, ctx, for_inbox).await?;
                Ok(None)
            }
            KnownObject::Delete(activity) => {
                ingest_delete(Verified(activity), ctx).await?;
                Ok(None)
            }
            KnownObject::Document(obj) => {
                ingest_postlike(Verified(KnownObject::Document(obj)), found_from, ctx).await
            }
            KnownObject::Event(obj) => {
                ingest_postlike(Verified(KnownObject::Event(obj)), found_from, ctx).await
            }
            KnownObject::Flag(activity) => {
                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing ID in activity"))?;

                let actor_ap_id = activity.actor_unchecked().as_single_id().ok_or(
                    crate::Error::InternalStrStatic("Missing actor for activity"),
                )?;

                crate::apub_util::require_containment(activity_id, actor_ap_id)?;

                let actor_local_id =
                    crate::apub_util::get_or_fetch_user_local_id(actor_ap_id, &db, &ctx).await?;

                let target =
                    activity
                        .object()
                        .as_single_id()
                        .ok_or(crate::Error::InternalStrStatic(
                            "Missing target in activity",
                        ))?;

                let target_found = if let Some(remaining) =
                    super::try_strip_host(target, &ctx.host_url_apub)
                {
                    super::LocalObjectRef::try_from_path(remaining).map(|x| (x, None))
                } else {
                    let row = db.query_opt(
                    "SELECT post.id, community.id, community.local, community.ap_id FROM post LEFT OUTER JOIN community ON (community.id = post.community) WHERE post.ap_id = $1",
                    &[&target.as_str()],
                ).await?;

                    row.map(|row| {
                        let post_id = PostLocalID(row.get(0));

                        let community_ap_id = if let Some(community_local) = row.get(2) {
                            if community_local {
                                let community_id = CommunityLocalID(row.get(1));

                                Some(Some(
                                    super::LocalObjectRef::Community(community_id)
                                        .to_local_uri(&ctx.host_url_apub),
                                ))
                            } else {
                                Some(row.get::<_, Option<&str>>(3).and_then(|x| x.parse().ok()))
                            }
                        } else {
                            Some(None)
                        };

                        (super::LocalObjectRef::Post(post_id), community_ap_id)
                    })
                };

                let content = activity
                    .content()
                    .as_ref()
                    .and_then(|x| x.as_one())
                    .and_then(|x| x.as_xsd_string());

                if let Some((target_local_id, community_ap_id)) = target_found {
                    match target_local_id {
                        super::LocalObjectRef::Post(post_id) => {
                            let community_ap_id = if let Some(community_ap_id) = community_ap_id {
                                community_ap_id
                            } else {
                                let row = db.query_opt(
                                "SELECT id, local, ap_id FROM community WHERE id = (SELECT community FROM post WHERE id=$1)",
                                &[&post_id],
                            ).await?;

                                row.and_then(|row| {
                                    if let Some(community_local) = row.get(1) {
                                        if community_local {
                                            let community_id = CommunityLocalID(row.get(0));

                                            Some(
                                                super::LocalObjectRef::Community(community_id)
                                                    .to_local_uri(&ctx.host_url_apub),
                                            )
                                        } else {
                                            row.get::<_, Option<&str>>(2)
                                                .and_then(|x| x.parse().ok())
                                        }
                                    } else {
                                        None
                                    }
                                })
                            };

                            let to_community = match community_ap_id {
                                None => false,
                                Some(community_ap_id) => {
                                    if let Some(to) = activity.to() {
                                        to.iter().any(|x| {
                                            x.as_xsd_any_uri().map(|x| x.as_str())
                                                == Some(community_ap_id.as_str())
                                        })
                                    } else {
                                        false
                                    }
                                }
                            };

                            db.execute(
                            "INSERT INTO flag (kind, person, post, content_text, to_community, to_remote_site_admin, created_local, local, ap_id) VALUES ('post', $1, $2, $3, $4, TRUE, current_timestamp, FALSE, $5) ON CONFLICT (ap_id) DO UPDATE SET kind='post', person=$1, post=$2, content_text=$3, to_community=$4",
                            &[&actor_local_id, &post_id, &content, &to_community, &activity_id.as_str()],
                        ).await?;
                        }
                        _ => {
                            log::warn!("unsupported flag target: {target_local_id:?}");
                        }
                    }
                }

                Ok(None)
            }
            KnownObject::Follow(follow) => {
                ingest_followlike(Verified(FollowLike::Follow(follow)), ctx).await?;

                Ok(None)
            }
            KnownObject::Group(group) => {
                let ap_id = group
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing ID in Group"))?;

                let name = group
                    .preferred_username()
                    .or_else(|| {
                        group
                            .name()
                            .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()))
                    })
                    .unwrap_or("");
                let description_html = group
                    .summary()
                    .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()));
                let inbox = group.inbox_unchecked().as_str();
                let outbox = group.outbox_unchecked();
                let followers = group.followers_unchecked().map(|x| x.as_str());
                let shared_inbox = group
                    .endpoints_unchecked()
                    .and_then(|endpoints| endpoints.shared_inbox.as_ref())
                    .map(|url| url.as_str());
                let public_key = group
                    .ext_one
                    .public_key
                    .as_ref()
                    .map(|key| key.public_key_pem.as_bytes());
                let public_key_sigalg = group
                    .ext_one
                    .public_key
                    .as_ref()
                    .and_then(|key| key.signature_algorithm.as_deref());

                ingest_remote_community_actor(
                    RemoteCommunityActor {
                        ap_id,
                        name,
                        description_html,
                        inbox,
                        outbox,
                        followers,
                        shared_inbox,
                        public_key,
                        public_key_sigalg,
                        featured: group.ext_two.featured.clone(),
                    },
                    &found_from,
                    ctx,
                )
                .await
            }
            KnownObject::Image(obj) => {
                ingest_postlike(Verified(KnownObject::Image(obj)), found_from, ctx).await
            }
            KnownObject::FunkwhaleLibrary(library) => {
                drop(db);
                ingest_funkwhale_library(library, ctx).await
            }
            KnownObject::Join(follow) => {
                ingest_followlike(Verified(FollowLike::Join(follow)), ctx).await?;

                Ok(None)
            }
            KnownObject::Leave(activity) => {
                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

                let actor_id = activity.actor_unchecked().as_single_id().ok_or(
                    crate::Error::InternalStrStatic("Missing actor for activity"),
                )?;

                let target_id = activity.object().as_single_id();

                super::require_containment(activity_id, actor_id)?;

                if let Some(target_id) = target_id {
                    if let Some(super::LocalObjectRef::Community(community_id)) =
                        super::LocalObjectRef::try_from_uri(target_id, &ctx.host_url_apub)
                    {
                        let follower_local_id = {
                            let row = db
                                .query_opt(
                                    "SELECT id FROM person WHERE ap_id=$1",
                                    &[&actor_id.as_str()],
                                )
                                .await?;
                            row.map(|row| UserLocalID(row.get(0)))
                        };
                        if let Some(follower_local_id) = follower_local_id {
                            db.execute(
                                "DELETE FROM community_follow WHERE community=$1 AND follower=$2",
                                &[&community_id, &follower_local_id],
                            )
                            .await?;
                        }
                    }
                }

                Ok(None)
            }
            KnownObject::Like(activity) => {
                ingest_like(Verified(activity), ctx).await?;
                Ok(None)
            }
            KnownObject::Note(obj) => {
                // try to handle poll response
                if let Some(in_reply_to) = obj.in_reply_to().and_then(|x| x.as_single_id()) {
                    if let Some(crate::apub_util::LocalObjectRef::Post(post_id)) =
                        crate::apub_util::LocalObjectRef::try_from_uri(
                            in_reply_to,
                            &ctx.host_url_apub,
                        )
                    {
                        if let Some(name) = obj
                            .name()
                            .as_ref()
                            .and_then(|x| x.as_one())
                            .and_then(|x| x.as_xsd_string())
                        {
                            if let Some(actor_id) = postlike_author_id(obj.attributed_to()) {
                                super::require_containment(
                                    obj.id_unchecked().ok_or(crate::Error::InternalStrStatic(
                                        "Missing activity ID",
                                    ))?,
                                    &actor_id,
                                )?;

                                let row = db.query_opt("SELECT poll_option.id, poll.id, poll.multiple, COALESCE(poll.is_closed, poll.closed_at <= current_timestamp, FALSE) FROM poll_option INNER JOIN poll ON (poll.id = poll_option.poll_id) WHERE poll_id=(SELECT poll_id FROM post WHERE id=$1 AND local) AND name=$2", &[&post_id, &name]).await?;
                                if let Some(row) = row {
                                    let option_id: i64 = row.get(0);
                                    let poll_id: i64 = row.get(1);
                                    let multiple: bool = row.get(2);
                                    let closed: bool = row.get(3);

                                    if closed {
                                        // ignore
                                    } else {
                                        let actor_local_id =
                                            super::get_or_fetch_user_local_id(&actor_id, &db, &ctx)
                                                .await?;

                                        {
                                            let trans = db.transaction().await?;

                                            if !multiple {
                                                trans
                                                    .execute(
                                                        "DELETE FROM poll_vote WHERE person=$1",
                                                        &[&actor_local_id],
                                                    )
                                                    .await?;
                                            }

                                            trans.execute("INSERT INTO poll_vote (poll_id, option_id, person) VALUES ($1, $2, $3)", &[&poll_id, &option_id, &actor_local_id]).await?;

                                            trans.commit().await?;
                                        }
                                    }

                                    return Ok(None);
                                }
                            }
                        }
                    }
                }

                ingest_postlike(Verified(KnownObject::Note(obj)), found_from, ctx).await
            }
            KnownObject::Page(obj) => {
                ingest_postlike(Verified(KnownObject::Page(obj)), found_from, ctx).await
            }
            KnownObject::Person(person) => {
                ingest_actorlike(Verified(person), false, &found_from, ctx).await
            }
            KnownObject::Question(obj) => {
                ingest_postlike(Verified(KnownObject::Question(obj)), found_from, ctx).await
            }
            KnownObject::Remove(activity) => {
                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

                let target = activity
                    .target()
                    .and_then(|x| x.as_single_id())
                    .ok_or(crate::Error::InternalStrStatic("Missing target for Remove"))?;

                let community_ap_id = activity
                    .actor_unchecked()
                    .as_single_id()
                    .ok_or(crate::Error::InternalStrStatic("Missing actor for Remove"))?;

                let res = db
                    .query_opt(
                        "SELECT id, ap_outbox FROM community WHERE ap_id=$1",
                        &[&community_ap_id.as_str()],
                    )
                    .await?;
                let community_local_info: Option<(CommunityLocalID, Option<&str>)> = res
                    .as_ref()
                    .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

                if let Some((community_local_id, ap_outbox)) = community_local_info {
                    let target_is_outbox = if let Some(ap_outbox) = ap_outbox {
                        ap_outbox == target.as_str()
                    } else {
                        let actor =
                            crate::apub_util::fetch_actor(community_ap_id, ctx.clone()).await?;

                        if let crate::apub_util::ActorLocalInfo::Community { ap_outbox, .. } = actor
                        {
                            if let Some(ap_outbox) = ap_outbox {
                                ap_outbox.as_str() == target.as_str()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };

                    if target_is_outbox {
                        crate::apub_util::require_containment(activity_id, community_ap_id)?;
                        crate::apub_util::require_containment(target, community_ap_id)?;

                        let object_id = activity.object().as_single_id();

                        if let Some(object_id) = object_id {
                            if let Some(local_id) =
                                super::LocalObjectRef::try_from_uri(object_id, &ctx.host_url_apub)
                            {
                                if let super::LocalObjectRef::Post(local_post_id) = local_id {
                                    db.execute(
                                    "UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$3 WHERE id=$1 AND community=$2",
                                    &[&local_post_id, &community_local_id, &activity_id.as_str()],
                                ).await?;
                                }
                            } else {
                                db.execute("UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$2 WHERE ap_id=$1", &[&object_id.as_str(), &activity_id.as_str()])
                                .await?;
                            }
                        }
                    }
                }

                Ok(None)
            }
            KnownObject::Service(obj) => {
                ingest_actorlike(Verified(obj), true, &found_from, ctx).await
            }
            KnownObject::Application(obj) => {
                ingest_actorlike(Verified(obj), true, &found_from, ctx).await
            }
            KnownObject::Undo(activity) => {
                ingest_undo(Verified(activity), ctx).await?;
                Ok(None)
            }
            KnownObject::Update(activity) => {
                let activity_id = activity
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

                let object_id =
                    activity
                        .object()
                        .as_single_id()
                        .ok_or(crate::Error::InternalStrStatic(
                            "Missing object ID for Update",
                        ))?;

                crate::apub_util::require_containment(activity_id, object_id)?;

                let object_id = super::url_from_ap_id(object_id)?;

                crate::spawn_task(async move {
                    let row = db
                        .query_opt(
                            "SELECT 1 FROM community WHERE ap_id=$1 LIMIT 1",
                            &[&object_id.as_str()],
                        )
                        .await?;
                    if row.is_some() {
                        ctx.enqueue_task(&crate::tasks::FetchActor {
                            actor_ap_id: Cow::Owned(object_id),
                        })
                        .await?;
                    }

                    Ok(())
                });

                Ok(None)
            }
            KnownObject::Video(obj) => {
                ingest_postlike(Verified(KnownObject::Video(obj)), found_from, ctx).await
            }
        }
    })
}

pub fn ingest_object_boxed(
    object: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
    for_inbox: bool,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<IngestResult>, crate::Error>> + Send>> {
    ingest_object(Box::new(object), found_from, ctx, for_inbox)
}

async fn mark_local_like_posted(
    db: &tokio_postgres::Client,
    local_ref: super::LocalObjectRef,
) -> Result<bool, crate::Error> {
    match local_ref {
        super::LocalObjectRef::PostLike(post_id, user_id) => {
            db.execute(MARK_LOCAL_POST_LIKE_POSTED_SQL, &[&post_id, &user_id])
                .await?;

            Ok(true)
        }
        super::LocalObjectRef::CommentLike(comment_id, user_id) => {
            db.execute(MARK_LOCAL_REPLY_LIKE_POSTED_SQL, &[&comment_id, &user_id])
                .await?;

            Ok(true)
        }
        _ => Ok(false),
    }
}

async fn mark_local_announced_object_seen(
    db: &tokio_postgres::Client,
    object_id: &activitystreams::iri_string::types::IriString,
    found_from: &FoundFrom,
    ctx: &Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let Some(local_object) = super::LocalObjectRef::try_from_uri(object_id, &ctx.host_url_apub)
    else {
        return Ok(());
    };

    match local_object {
        super::LocalObjectRef::Post(local_post_id) => {
            mark_local_post_seen_from_remote(db, local_post_id, found_from).await?;
        }
        super::LocalObjectRef::Comment(local_comment_id) => {
            mark_local_comment_seen_from_remote(db, local_comment_id, found_from).await?;
        }
        super::LocalObjectRef::PostLike(_, _) | super::LocalObjectRef::CommentLike(_, _) => {
            mark_local_like_posted(db, local_object).await?;
        }
        _ => {}
    }

    Ok(())
}

fn local_announced_object_id(
    value: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
    host_url_apub: &crate::BaseURL,
) -> Option<activitystreams::iri_string::types::IriString> {
    let object_id = followlike_id(value)?;

    match super::LocalObjectRef::try_from_uri(&object_id, host_url_apub) {
        Some(
            super::LocalObjectRef::Post(_)
            | super::LocalObjectRef::Comment(_)
            | super::LocalObjectRef::PostLike(_, _)
            | super::LocalObjectRef::CommentLike(_, _),
        ) => Some(object_id),
        _ => None,
    }
}

pub async fn ingest_like(
    activity: Verified<activitystreams::activity::Like>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let mut db = ctx.db_pool.get().await?;

    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    if let Some(local_ref) = super::LocalObjectRef::try_from_uri(activity_id, &ctx.host_url_apub) {
        if mark_local_like_posted(&db, local_ref).await? {
            return Ok(());
        }
    }

    if let Some(actor_id) = followlike_id(activity.actor_unchecked()) {
        if !super::is_contained(activity_id, &actor_id) {
            log::warn!("Like activity ID {activity_id} does not share actor origin {actor_id}");
        }

        let actor_local_id = super::get_or_fetch_user_local_id(&actor_id, &db, &ctx).await?;

        if let Some(object_id) = followlike_id(activity.object()) {
            let thing_local_ref = if let Some(local_id) =
                super::LocalObjectRef::try_from_uri(&object_id, &ctx.host_url_apub)
            {
                match local_id {
                    super::LocalObjectRef::Post(id) => Some(ThingLocalRef::Post(id)),
                    super::LocalObjectRef::Comment(id) => Some(ThingLocalRef::Comment(id)),
                    _ => None,
                }
            } else {
                let row = db.query_opt(
                    "(SELECT TRUE, id FROM post WHERE ap_id=$1) UNION ALL (SELECT FALSE, id FROM reply WHERE ap_id=$1) LIMIT 1",
                    &[&object_id.as_str()],
                ).await?;

                row.map(|row| {
                    if row.get(0) {
                        ThingLocalRef::Post(PostLocalID(row.get(1)))
                    } else {
                        ThingLocalRef::Comment(CommentLocalID(row.get(1)))
                    }
                })
            };

            match thing_local_ref {
                Some(ThingLocalRef::Post(post_local_id)) => {
                    let is_new = {
                        let mut trans = db.transaction().await?;

                        let row_count = trans.execute(
                            "INSERT INTO post_like (post, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (post, person) DO NOTHING",
                            &[&post_local_id, &actor_local_id, &activity_id.as_str()],
                        ).await?;

                        let is_new = row_count > 0;

                        if is_new {
                            crate::recalculate_cached_post_likes(&mut trans, post_local_id).await?;
                        }

                        trans.commit().await?;

                        is_new
                    };

                    if is_new {
                        let row = db.query_opt("SELECT post.community, community.local FROM post, community WHERE post.community = community.id AND post.id=$1", &[&post_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = CommunityLocalID(row.get(0));
                                let body = serde_json::to_string(&activity)?;
                                super::enqueue_forward_to_community_followers(
                                    community_id,
                                    body,
                                    ctx,
                                )
                                .await?;
                            }
                        }
                    }
                }
                Some(ThingLocalRef::Comment(comment_local_id)) => {
                    let row_count = db.execute(
                        "INSERT INTO reply_like (reply, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (reply, person) DO NOTHING",
                        &[&comment_local_id, &actor_local_id, &activity_id.as_str()],
                    ).await?;

                    if row_count > 0 {
                        let row = db.query_opt("SELECT post.community, community.local FROM reply, post, community WHERE reply.post = post.id AND post.community = community.id AND reply.id=$1", &[&comment_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = CommunityLocalID(row.get(0));
                                let body = serde_json::to_string(&activity)?;
                                super::enqueue_forward_to_community_followers(
                                    community_id,
                                    body,
                                    ctx,
                                )
                                .await?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

pub async fn ingest_delete(
    activity: Verified<activitystreams::activity::Delete>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing ID for activity"))?;
    let actor_id = activity
        .actor_unchecked()
        .as_single_id()
        .ok_or(crate::Error::InternalStrStatic("Missing ID for actor"))?;

    if let Some(object_id) = activity.object().as_single_id() {
        super::require_containment(activity_id, actor_id)?;
        super::require_containment(object_id, actor_id)?;

        // maybe it's a post or reply
        let row = db.query_opt(
            "WITH deleted_post AS (UPDATE post SET href=NULL, title='[deleted]', content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id = post.community AND community.local)), deleted_reply AS (UPDATE reply SET content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id=(SELECT community FROM post WHERE id=reply.post) AND community.local)) (SELECT * FROM deleted_post) UNION ALL (SELECT * FROM deleted_reply) LIMIT 1",
            &[&object_id.as_str()],
            ).await?;

        if let Some(row) = row {
            // Something was deleted
            let local_community = row.get::<_, Option<_>>(0).map(CommunityLocalID);
            if let Some(community_id) = local_community {
                // Community is local, need to forward delete to followers

                let body = serde_json::to_string(&activity)?;

                crate::spawn_task(crate::apub_util::enqueue_forward_to_community_followers(
                    community_id,
                    body,
                    ctx,
                ));
            }
        } else {
            // maybe it's a community
            db.execute("UPDATE community SET deleted=TRUE, old_name=name, name='[deleted]', description=NULL, description_html=NULL, description_markdown=NULL, created_by=NULL, public_key=NULL WHERE ap_id=$1", &[&object_id.as_str()]).await?;
        }
    }

    Ok(())
}

pub async fn ingest_undo(
    activity: Verified<activitystreams::activity::Undo>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    let actor_id = followlike_id(activity.actor_unchecked()).ok_or(
        crate::Error::InternalStrStatic("Missing actor for activity"),
    )?;

    let object_id = followlike_id(activity.object())
        .ok_or(crate::Error::InternalStrStatic("Missing object for Undo"))?;

    if !super::is_contained(activity_id, &actor_id) {
        log::warn!("Undo activity ID {activity_id} does not share actor origin {actor_id}");
    }

    if !super::is_contained(&object_id, &actor_id) {
        log::warn!("Undo object ID {object_id} does not share actor origin {actor_id}");
    }

    let object_id = object_id.as_str();

    let mut db = ctx.db_pool.get().await?;

    let mut trans = db.transaction().await?;

    {
        let result = trans
            .query_opt(
                "DELETE FROM post_like WHERE ap_id=$1 RETURNING post",
                &[&object_id],
            )
            .await?;

        if let Some(row) = result {
            let post_id = PostLocalID(row.get(0));

            crate::recalculate_cached_post_likes(&mut trans, post_id).await?;
        }
    }
    trans
        .execute("DELETE FROM reply_like WHERE ap_id=$1", &[&object_id])
        .await?;
    trans
        .execute("DELETE FROM community_follow WHERE ap_id=$1", &[&object_id])
        .await?;
    trans
        .execute(
            "DELETE FROM collection_target_follow WHERE ap_id=$1",
            &[&object_id],
        )
        .await?;
    trans
        .execute("DELETE FROM person_follow WHERE ap_id=$1", &[&object_id])
        .await?;
    trans.execute(
        "UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$2 WHERE approved_ap_id=$1",
        &[&object_id, &activity_id.as_str()],
    )
    .await?;

    trans.commit().await?;

    Ok(())
}

pub async fn ingest_create(
    activity: Verified<activitystreams::activity::Create>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
    for_inbox: bool,
) -> Result<(), crate::Error> {
    for req_obj in activity.object() {
        let object_id = req_obj.id();

        if let Some(object_id) = object_id {
            let obj = if activity.id_unchecked().is_some_and(|activity_id| {
                embedded_activity_object_is_trusted(activity_id, object_id)
            }) {
                Verified(
                    serde_json::from_value(serde_json::to_value(&req_obj)?).map_err(|err| {
                        log::debug!("Failed to parse incoming message: {err:?}");
                        crate::Error::UserError(crate::simple_response(
                            hyper::StatusCode::FORBIDDEN,
                            "Invalid or unsupported data",
                        ))
                    })?,
                )
            } else {
                crate::apub_util::fetch_ap_object(object_id, &ctx).await?
            };

            ingest_object_boxed(obj, found_from.clone(), ctx.clone(), for_inbox).await?;
        }
    }

    Ok(())
}

pub struct PollIngestInfo {
    multiple: bool,
    is_closed: Option<bool>,
    closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    options: Vec<(String, Option<i32>)>,
}

fn extract_url_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.to_owned()),
        serde_json::Value::Array(values) => values.iter().find_map(extract_url_value),
        serde_json::Value::Object(map) => map
            .get("href")
            .or_else(|| map.get("url"))
            .and_then(extract_url_value),
        _ => None,
    }
}

fn get_attachment_href(
    base: &activitystreams::base::AnyBase,
) -> Result<Option<String>, crate::Error> {
    let href = match base.kind_str() {
        Some("Document") => Some(
            activitystreams::object::Document::from_any_base(base.clone()).map(|obj| {
                obj.unwrap()
                    .take_url()
                    .as_ref()
                    .and_then(|href| href.iter().find_map(|x| x.as_xsd_any_uri()))
                    .map(|href| href.as_str().to_owned())
            }),
        ),
        Some("Image") => Some(
            activitystreams::object::Image::from_any_base(base.clone()).map(|obj| {
                obj.unwrap()
                    .take_url()
                    .as_ref()
                    .and_then(|href| href.iter().find_map(|x| x.as_xsd_any_uri()))
                    .map(|href| href.as_str().to_owned())
            }),
        ),
        Some("Link") => Some(
            activitystreams::link::Link::<activitystreams::link::kind::LinkType>::from_any_base(
                base.clone(),
            )
            .map(|obj| {
                obj.unwrap()
                    .take_href()
                    .map(|href| href.as_str().to_owned())
            }),
        ),
        _ => None,
    }
    .transpose()?
    .flatten();

    if href.is_some() {
        return Ok(href);
    }

    let value = serde_json::to_value(base)?;

    Ok(value
        .get("href")
        .or_else(|| value.get("url"))
        .and_then(extract_url_value))
}

fn post_replies_value(
    replies: Option<&activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>>,
) -> Result<Option<serde_json::Value>, crate::Error> {
    replies
        .map(serde_json::to_value)
        .transpose()
        .map_err(Into::into)
}

async fn enqueue_post_background_fetches_if_present(
    post: &PostIngestResult,
    replies: Option<serde_json::Value>,
    post_ap_id: Option<&str>,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    if let Some(replies) = replies {
        crate::tasks::enqueue_post_replies_fetch(post.id, replies, ctx.clone()).await?;
    }

    if let Some(post_ap_id) = post_ap_id {
        crate::tasks::enqueue_platform_post_thread_fetch(post.id, post_ap_id, ctx).await?;
    }

    Ok(())
}

async fn community_from_blog_publisher_author(
    author_ap_id: Option<&activitystreams::iri_string::types::IriString>,
    found_from: &FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<(CommunityLocalID, bool)>, crate::Error> {
    let Some(author_ap_id) = author_ap_id else {
        return Ok(None);
    };

    let db = ctx.db_pool.get().await?;
    if let Some(row) = db
        .query_opt(
            "SELECT id, local FROM community WHERE ap_id=$1 AND NOT deleted",
            &[&author_ap_id.as_str()],
        )
        .await?
    {
        return Ok(Some((CommunityLocalID(row.get(0)), row.get(1))));
    }
    drop(db);

    if !found_from.keeps_untracked_remote_group() {
        return Ok(None);
    }

    let author_url = super::url_from_ap_id(author_ap_id)?;
    match super::fetch_actor_for_explicit_lookup(&author_url, ctx.clone()).await {
        Ok(super::ActorLocalInfo::Community { id, .. }) => {
            let db = ctx.db_pool.get().await?;
            let row = db
                .query_opt("SELECT local FROM community WHERE id=$1", &[&id])
                .await?;

            Ok(row.map(|row| (id, row.get(0))))
        }
        Ok(super::ActorLocalInfo::User { .. }) => Ok(None),
        Err(err) => {
            log::debug!(
                "Could not resolve blog publisher author {author_ap_id} as community: {err:?}"
            );
            Ok(None)
        }
    }
}

/// Ingestion flow for Page, Image, Article, and Note. Should not be called with any other objects.
async fn ingest_postlike(
    obj: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let (ext, to, in_reply_to, obj_id, poll_info, tag, maybe_url, attachment, cc, replies) =
        match &*obj {
            KnownObject::Page(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                obj.url(),
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Image(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                None,
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Audio(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                obj.url(),
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Article(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                None,
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Document(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                obj.url(),
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Event(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                obj.url(),
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Note(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                obj.in_reply_to(),
                obj.id_unchecked(),
                None,
                obj.tag(),
                None,
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Video(obj) => (
                Some(&obj.ext_one),
                obj.to(),
                None,
                obj.id_unchecked(),
                None,
                obj.tag(),
                obj.url(),
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            KnownObject::Question(obj) => (
                None,
                obj.to(),
                obj.in_reply_to(),
                obj.id_unchecked(),
                Some({
                    #[derive(Deserialize)]
                    struct OptionObject {
                        name: String,
                        replies: Option<crate::apub_util::AnyCollection>,
                    }

                    let (multiple, options) = if let Some(any_of) = obj.any_of() {
                        (true, any_of)
                    } else if let Some(one_of) = obj.one_of() {
                        (false, one_of)
                    } else {
                        return Err(crate::Error::InternalStrStatic("Invalid poll"));
                    };

                    let options = options
                        .iter()
                        .map(|value| serde_json::from_value(serde_json::to_value(value)?))
                        .collect::<Result<Vec<OptionObject>, _>>()?;
                    let options = options
                        .into_iter()
                        .map(|value| {
                            let remote_count = value.replies.and_then(|coll| coll.total_items());
                            (value.name, remote_count.map(crate::u64_to_i32_saturating))
                        })
                        .collect();

                    let (is_closed, closed_at) = match obj.closed() {
                        Some(value) => match value {
                            activitystreams::primitives::Either::Left(_) => (None, None),
                            activitystreams::primitives::Either::Right(
                                activitystreams::primitives::Either::Left(timestamp),
                            ) => (None, Some(super::offset_datetime_to_chrono(&timestamp))),
                            activitystreams::primitives::Either::Right(
                                activitystreams::primitives::Either::Right(value),
                            ) => (Some(value), None),
                        },
                        None => (None, None),
                    };

                    PollIngestInfo {
                        multiple,
                        is_closed,
                        closed_at,
                        options,
                    }
                }),
                obj.tag(),
                None,
                obj.attachment(),
                obj.cc(),
                post_replies_value(obj.replies())?,
            ),
            _ => return Ok(None), // shouldn't happen?
        };
    let target = ext.as_ref().and_then(|x| x.target.as_ref());
    let observed_object_type = super::target::known_object_type(&obj);
    let author_for_community = match &*obj {
        KnownObject::Page(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Image(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Audio(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Article(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Document(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Event(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Note(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Video(obj) => postlike_author_id(obj.attributed_to()),
        KnownObject::Question(obj) => postlike_author_id(obj.attributed_to()),
        _ => None,
    };

    let public = to
        .iter()
        .flat_map(|x| x.iter())
        .chain(cc.iter().flat_map(|x| x.iter()))
        .any(|x| match x.as_xsd_any_uri() {
            Some(uri) => uri == &activitystreams::public(),
            None => false,
        });

    if !public {
        log::debug!(
            "refusing to ingest non-public object {:?}",
            obj_id.map(|x| x.as_str())
        );
        return Ok(None);
    }

    let mentions = {
        let tag = match tag {
            None => vec![],
            Some(value) => value.iter().collect(),
        };

        let mut mentions = Vec::new();

        let mut map: HashMap<url::Url, String> = tag
            .into_iter()
            .filter_map(|tag| {
                if let Ok(Some::<activitystreams::link::Mention>(mut mention)) =
                    tag.clone().extend()
                {
                    if let Some(url) = mention.take_href() {
                        if let Some(name) = mention
                            .name()
                            .as_ref()
                            .and_then(|x| x.as_single_xsd_string())
                        {
                            return Some((super::url_from_ap_id(&url).ok()?, name.to_owned()));
                        }
                    }
                }

                None
            })
            .filter_map(|(url, text)| {
                if let Some(local_ref) =
                    crate::apub_util::LocalObjectRef::try_from_uri(&url, &ctx.host_url_apub)
                {
                    if let crate::apub_util::LocalObjectRef::User(user_id) = local_ref {
                        mentions.push(crate::MentionInfo {
                            text,
                            person: user_id,
                            ap_id: crate::APIDOrLocal::Local,
                        });
                    }

                    None
                } else {
                    Some((url, text))
                }
            })
            .collect();

        log::debug!("handling mentions: {} {}", mentions.len(), map.len());

        if !map.is_empty() {
            let urls: Vec<&str> = map.keys().map(url::Url::as_str).collect();

            log::debug!("looking up mentioned users {urls:?}");

            let db = ctx.db_pool.get().await?;

            let rows = db
                .query(
                    "SELECT id, ap_id FROM person WHERE ap_id=ANY($1::TEXT[])",
                    &[&urls],
                )
                .await?;

            mentions.extend(rows.into_iter().map(|row| {
                let ap_id: &str = row.get(1);
                let ap_id: url::Url = ap_id.parse().unwrap();

                crate::MentionInfo {
                    text: map.remove(&ap_id).unwrap(),
                    person: UserLocalID(row.get(0)),
                    ap_id: crate::APIDOrLocal::APID(ap_id),
                }
            }));
        }

        mentions
    };

    // Interpret attachments (usually images) as links
    let href = attachment
        .and_then(|x| x.iter().next())
        .map(get_attachment_href)
        .transpose()?
        .flatten()
        .or_else(|| {
            maybe_url
                .and_then(|href| href.iter().find_map(|x| x.as_xsd_any_uri()))
                .map(|href| href.as_str().to_owned())
        });

    let community_found = match target
        .as_ref()
        .and_then(|target| target.as_one().and_then(|x| x.id()))
        .map(|target_id| {
            if let Some(super::LocalObjectRef::CommunityOutbox(community_local_id)) =
                super::LocalObjectRef::try_from_uri(target_id, &ctx.host_url_apub)
            {
                Some(community_local_id)
            } else {
                None
            }
        }) {
        Some(Some(community_local_id)) => Some((community_local_id, true)),
        Some(None) | None => {
            if let Some(community) = found_from.community() {
                Some(community)
            } else if matches!(found_from, FoundFrom::Refresh) {
                if let Some(obj_id) = obj_id {
                    let db = ctx.db_pool.get().await?;

                    let row = db.query_opt("SELECT id, local FROM community WHERE id=(SELECT community FROM post WHERE ap_id=$1)", &[&obj_id.as_str()]).await?;
                    row.map(|row| (CommunityLocalID(row.get(0)), row.get(1)))
                } else {
                    None
                }
            } else {
                match to {
                    None => None,
                    Some(maybe) => maybe.iter().find_map(|any| {
                        any.as_xsd_any_uri()
                            .and_then(|uri| {
                                if let Some(super::LocalObjectRef::Community(community_id)) =
                                    super::LocalObjectRef::try_from_uri(uri, &ctx.host_url_apub)
                                {
                                    Some(community_id)
                                } else {
                                    None
                                }
                            })
                            .map(|id| (id, true))
                    }),
                }
            }
        }
    };
    let community_found = if community_found.is_none() && in_reply_to.is_none() {
        community_from_blog_publisher_author(
            author_for_community.as_ref(),
            &found_from,
            ctx.clone(),
        )
        .await?
    } else {
        community_found
    };

    let approved = community_found.is_some_and(|(_, community_is_local)| community_is_local)
        || found_from.approves_post();
    let allow_untracked_remote_community = found_from.allows_untracked_remote_community();
    let post_ap_id = obj_id.map(|id| id.as_str().to_owned());

    if let Some((community_local_id, community_is_local)) = community_found {
        if !community_is_local {
            if let Some(object_type) = observed_object_type {
                record_community_observed_object_type(community_local_id, object_type, ctx.clone())
                    .await?;
            }
        }

        match obj.into_inner() {
            KnownObject::Page(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(try_transform_inner(obj)?),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Image(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(try_transform_inner(obj)?),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Audio(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(obj),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Article(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(try_transform_inner(obj)?),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Document(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(obj),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Event(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(obj),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Question(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(try_transform_inner(obj)?),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Video(obj) => {
                let post = handle_received_page_for_community(
                    community_local_id,
                    community_is_local,
                    allow_untracked_remote_community,
                    approved,
                    found_from.approved_ap_id(),
                    poll_info,
                    mentions,
                    href,
                    Verified(obj),
                    ctx.clone(),
                )
                .await?;

                if let Some(post) = &post {
                    enqueue_post_background_fetches_if_present(
                        post,
                        replies,
                        post_ap_id.as_deref(),
                        ctx.clone(),
                    )
                    .await?;
                }

                Ok(post.map(IngestResult::Post))
            }
            KnownObject::Note(obj) => {
                let content = obj.content();
                let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
                let media_type = obj.media_type();
                let created = obj.published();
                let author = postlike_author_id(obj.attributed_to());

                if let Some(object_id) = obj.id_unchecked() {
                    let sensitive = obj.ext_two.sensitive;

                    if let Some(in_reply_to) = obj.in_reply_to() {
                        // it's a reply

                        // fetch first attachment
                        let attachment_href = obj
                            .attachment()
                            .and_then(|x| x.iter().next())
                            .map(get_attachment_href)
                            .transpose()?
                            .flatten();

                        Ok(handle_recieved_reply(
                            Verified(KnownObject::Note(obj.clone())),
                            object_id,
                            content.unwrap_or(""),
                            media_type,
                            created.as_ref(),
                            author.as_ref(),
                            in_reply_to,
                            attachment_href.as_deref(),
                            sensitive,
                            mentions,
                            &found_from,
                            ctx,
                        )
                        .await?
                        .map(|id| IngestResult::Other(ThingLocalRef::Comment(id))))
                    } else {
                        // not a reply, must be a top-level post
                        if !remote_community_post_is_wanted(
                            community_local_id,
                            community_is_local,
                            allow_untracked_remote_community,
                            ctx.clone(),
                        )
                        .await?
                        {
                            return Ok(None);
                        }

                        let summary = obj.summary();
                        let name = obj.name();

                        let title = name
                            .as_ref()
                            .and_then(|x| x.as_single_xsd_string())
                            .or_else(|| summary.as_ref().and_then(|x| x.as_single_xsd_string()))
                            .unwrap_or("");

                        let sensitive = obj.ext_two.sensitive;

                        let post = handle_recieved_post(
                            object_id.clone(),
                            title,
                            href.as_deref(),
                            content,
                            media_type,
                            created.as_ref(),
                            author,
                            community_local_id,
                            community_is_local,
                            approved,
                            found_from.approved_ap_id(),
                            poll_info,
                            sensitive,
                            mentions,
                            ctx.clone(),
                        )
                        .await?;

                        enqueue_post_background_fetches_if_present(
                            &post,
                            replies,
                            post_ap_id.as_deref(),
                            ctx.clone(),
                        )
                        .await?;

                        Ok(Some(IngestResult::Post(post)))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Err(crate::Error::InternalStrStatic(
                "ingest_postlike called with an unknown object",
            )),
        }
    } else {
        // not to a community, but might still match as a reply
        if let Some(in_reply_to) = in_reply_to {
            if let Some(obj_id) = obj_id {
                if let KnownObject::Note(obj) = &*obj {
                    // TODO deduplicate this?

                    let content = obj.content();
                    let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
                    let media_type = obj.media_type();
                    let created = obj.published();
                    let author = postlike_author_id(obj.attributed_to());

                    if let Some(author) = &author {
                        require_containment_or_mbin_mirror_source(
                            obj_id,
                            author,
                            obj.ext_three.lotide_mbin_source_id.as_ref(),
                        )?;
                    }

                    // fetch first attachment
                    let attachment_href = obj
                        .attachment()
                        .and_then(|x| x.iter().next())
                        .map(get_attachment_href)
                        .transpose()?
                        .flatten();
                    let sensitive = obj.ext_two.sensitive;

                    let id = handle_recieved_reply(
                        Verified(KnownObject::Note(obj.clone())),
                        obj_id,
                        content.unwrap_or(""),
                        media_type,
                        created.as_ref(),
                        author.as_ref(),
                        in_reply_to,
                        attachment_href.as_deref(),
                        sensitive,
                        mentions,
                        &found_from,
                        ctx,
                    )
                    .await?;

                    Ok(id.map(|id| IngestResult::Other(ThingLocalRef::Comment(id))))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            log::debug!("Couldn't find community for post");
            Ok(None)
        }
    }
}

async fn ingest_followlike(
    follow: Verified<FollowLike>,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let follower_ap_id = followlike_id(follow.actor_unchecked());
    let target = followlike_id(follow.object());
    if follower_ap_id.is_none() {
        log::warn!("Ignoring follow-like with missing actor id");
        return Ok(());
    }

    let follower_ap_id = follower_ap_id.unwrap();
    let db = ctx.db_pool.get().await?;

    let follower_local_id =
        crate::apub_util::get_or_fetch_user_local_id(&follower_ap_id, &db, &ctx).await?;

    /*
        Follow and Join activities are accepted for historical compatibility,
        but a usable row still needs an activity ID for later Accept or Undo
        processing. When a local target is clear and the remote actor omitted
        the ID, derive the local follow ID rather than dropping the request.
    */
    let activity_ap_id = match follow.id_unchecked() {
        Some(activity_ap_id) => {
            if crate::apub_util::is_contained(activity_ap_id, &follower_ap_id) {
                Some(activity_ap_id.clone())
            } else {
                log::warn!(
                    "Follow-like activity ID {activity_ap_id} does not share actor origin {follower_ap_id}; deriving local follow id"
                );
                None
            }
        }
        None => {
            if let Some(target) = target.as_ref() {
                if let Some(super::LocalObjectRef::Community(community_id)) =
                    super::LocalObjectRef::try_from_uri(target, &ctx.host_url_apub)
                {
                    log::warn!(
                        "Follow activity missing ID from {follower_ap_id}, generating fallback community follow id"
                    );

                    Some(
                        super::LocalObjectRef::CommunityFollow(community_id, follower_local_id)
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    )
                } else if let Some(super::LocalObjectRef::User(target_user_id)) =
                    super::LocalObjectRef::try_from_uri(&target, &ctx.host_url_apub)
                {
                    log::warn!(
                        "Follow activity missing ID from {follower_ap_id}, generating fallback user follow id"
                    );

                    Some(
                        super::LocalObjectRef::UserFollow(target_user_id, follower_local_id)
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    )
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    let activity_ap_id = if let Some(activity_ap_id) = activity_ap_id {
        activity_ap_id
    } else {
        log::warn!(
            "Ignoring follow-like with unknown target {} (no activity id available)",
            target
                .as_ref()
                .map_or_else(|| follower_ap_id.as_str(), |target| target.as_str())
        );
        return Ok(());
    };

    if let Some(target) = target.as_ref() {
        if let Some(super::LocalObjectRef::Community(community_id)) =
            super::LocalObjectRef::try_from_uri(&target, &ctx.host_url_apub)
        {
            let row = db
                .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                .await?;
            if let Some(row) = row {
                let local: bool = row.get(0);
                if local {
                    db.execute("INSERT INTO community_follow (community, follower, local, ap_id, accepted) VALUES ($1, $2, FALSE, $3, TRUE) ON CONFLICT (community, follower) DO UPDATE SET ap_id = $3, accepted = TRUE", &[&community_id, &follower_local_id, &activity_ap_id.as_str()]).await?;

                    crate::apub_util::spawn_enqueue_send_community_follow_accept(
                        community_id,
                        follower_local_id,
                        Some(activity_ap_id.clone()),
                        ctx.clone(),
                    );
                }
            } else {
                log::warn!("Ignoring follow-like with unknown community target {target}");
            }
        } else if let Some(super::LocalObjectRef::User(target_user_id)) =
            super::LocalObjectRef::try_from_uri(&target, &ctx.host_url_apub)
        {
            let row = db
                .query_opt("SELECT local FROM person WHERE id=$1", &[&target_user_id])
                .await?;
            if let Some(row) = row {
                let local: bool = row.get(0);
                if local {
                    db.execute("INSERT INTO person_follow (target, follower, local, ap_id, accepted) VALUES ($1, $2, FALSE, $3, TRUE) ON CONFLICT (target, follower) DO UPDATE SET ap_id = $3, accepted = TRUE", &[&target_user_id, &follower_local_id, &activity_ap_id.as_str()]).await?;

                    /*
                        Following a person is visible social activity, not just
                        delivery plumbing. Notify the local target once per
                        follower so repeated Follow retries do not spam them.
                    */
                    if let Some(row) = db
                        .query_opt(
                            "INSERT INTO notification (kind, created_at, to_user, from_user) \
                             SELECT 'user_follow', current_timestamp, $1, $2 \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM notification \
                                 WHERE kind='user_follow' \
                                 AND to_user=$1 \
                                 AND from_user=$2\
                             ) \
                             RETURNING id",
                            &[&target_user_id, &follower_local_id],
                        )
                        .await?
                    {
                        ctx.enqueue_task(&crate::tasks::SendNotification {
                            notification: NotificationID(row.get(0)),
                        })
                        .await?;
                    }

                    crate::apub_util::spawn_enqueue_send_user_follow_accept(
                        target_user_id,
                        follower_local_id,
                        Some(activity_ap_id.clone()),
                        ctx.clone(),
                    );
                }
            } else {
                log::warn!("Ignoring follow-like with unknown user target {target}");
            }
        } else {
            log::warn!("Ignoring follow-like for non-local target {target}");
        }
    } else {
        log::warn!("Ignoring follow-like {activity_ap_id} without a target");
    }

    Ok(())
}

fn followlike_id(
    value: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
) -> Option<activitystreams::iri_string::types::IriString> {
    one_or_many_ap_id(value)
}

fn postlike_author_id(
    value: Option<&activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>>,
) -> Option<activitystreams::iri_string::types::IriString> {
    value.and_then(one_or_many_ap_id)
}

fn one_or_many_ap_id(
    value: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
) -> Option<activitystreams::iri_string::types::IriString> {
    value.as_single_id().cloned().or_else(|| {
        value.iter().find_map(|candidate| {
            candidate.id().cloned().or_else(|| {
                candidate
                    .as_xsd_any_uri()
                    .and_then(|id| id.as_str().parse().ok())
            })
        })
    })
}

async fn ingest_remote_community_actor(
    actor: RemoteCommunityActor<'_>,
    found_from: &FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let id = CommunityLocalID(db.query_one(
        "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, created_local, ap_outbox, ap_followers) VALUES ($1, FALSE, $2, $3, $4, $5, $6, $7, current_timestamp, $8, $9) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description_html=$7, ap_outbox=$8, ap_followers=$9 RETURNING id",
        &[
            &actor.name,
            &actor.ap_id.as_str(),
            &actor.inbox,
            &actor.shared_inbox,
            &actor.public_key,
            &actor.public_key_sigalg,
            &actor.description_html,
            &actor.outbox.map(|outbox| outbox.as_str()),
            &actor.followers,
        ],
    ).await?.get(0));

    let outbox = actor.outbox.map(super::url_from_ap_id).transpose()?;

    let has_local_follow = db
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM community_follow WHERE community=$1 AND local AND accepted)",
            &[&id],
        )
        .await?
        .get(0);

    if has_local_follow || found_from.keeps_untracked_remote_group() {
        if let Some(featured_url) = actor.featured {
            crate::apub_util::spawn_enqueue_fetch_community_featured(id, featured_url, ctx.clone());
        }

        if let Some(outbox_url) = outbox.clone() {
            crate::apub_util::spawn_enqueue_fetch_community_outbox_preview(
                id,
                outbox_url,
                ctx.clone(),
            );
        }
    } else {
        let deleted_empty_community = db
            .execute(DELETE_EMPTY_UNTRACKED_REMOTE_COMMUNITY_SQL, &[&id])
            .await?;

        if deleted_empty_community > 0 {
            log::debug!(
                "discarded untracked remote group {} after incidental actor fetch",
                actor.ap_id
            );
        }
    }

    Ok(Some(IngestResult::Actor(
        super::ActorLocalInfo::Community {
            id,
            public_key: actor.public_key.map(|key| super::PubKeyInfo {
                algorithm: super::get_message_digest(actor.public_key_sigalg),
                key: key.to_owned(),
            }),
            ap_outbox: outbox,
        },
    )))
}

async fn ingest_funkwhale_library(
    library: super::FunkwhaleLibrary,
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    /*
        A Funkwhale Library is followable, but it is not itself an inbox owner.
        Store it as a collection target and keep the owner actor alongside it
        so follows and unfollows go to the actor that Funkwhale expects.
    */
    let library_ap_id = library.id().as_str();
    let owner_ap_id = library
        .owner_ap_id()
        .ok_or(crate::Error::InternalStrStatic(
            "Funkwhale Library object is missing attributedTo owner",
        ))?
        .to_owned();

    let owner_url: url::Url = owner_ap_id.parse()?;
    let owner = super::fetch_actor(&owner_url, ctx.clone()).await?;
    let owner_user = match owner {
        super::ActorLocalInfo::User { id, .. } => id,
        super::ActorLocalInfo::Community { .. } => {
            return Err(crate::Error::InternalStrStatic(
                "Funkwhale Library owner was not a user-like actor",
            ));
        }
    };

    let db = ctx.db_pool.get().await?;
    let owner_row = db
        .query_one(
            "SELECT ap_id, ap_inbox, ap_shared_inbox FROM person WHERE id=$1",
            &[&owner_user],
        )
        .await?;
    let owner_ap_id_from_db: Option<&str> = owner_row.get(0);
    let owner_inbox: Option<&str> = owner_row.get(1);
    let owner_shared_inbox: Option<&str> = owner_row.get(2);

    let display_name = library
        .str_field("name")
        .filter(|name| !name.trim().is_empty())
        .map_or_else(|| "Funkwhale library".to_owned(), str::to_owned);
    let target_kind = "funkwhale_library";
    let software = "funkwhale";
    let stored_owner_ap_id = owner_ap_id_from_db.unwrap_or(owner_ap_id.as_str());
    let followers = library.str_field("followers");
    let first_page = library.str_field("first");
    let last_page = library.str_field("last");
    let summary_html = library.str_field("summary");
    let total_items = library.i64_field("totalItems");

    let id = CollectionTargetLocalID(
        db.query_one(
            "INSERT INTO collection_target (
                name,
                target_kind,
                software,
                ap_id,
                owner_actor,
                owner_ap_id,
                owner_inbox,
                owner_shared_inbox,
                followers,
                first_page,
                last_page,
                summary_html,
                total_items,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, current_timestamp
            ) ON CONFLICT (ap_id) DO UPDATE SET
                name=$1,
                target_kind=$2,
                software=$3,
                owner_actor=$5,
                owner_ap_id=$6,
                owner_inbox=$7,
                owner_shared_inbox=$8,
                followers=$9,
                first_page=$10,
                last_page=$11,
                summary_html=$12,
                total_items=$13,
                updated_at=current_timestamp
            RETURNING id",
            &[
                &display_name,
                &target_kind,
                &software,
                &library_ap_id,
                &owner_user,
                &stored_owner_ap_id,
                &owner_inbox,
                &owner_shared_inbox,
                &followers,
                &first_page,
                &last_page,
                &summary_html,
                &total_items,
            ],
        )
        .await?
        .get(0),
    );

    if let Some(first_page) = first_page.and_then(|value| value.parse().ok()) {
        super::spawn_enqueue_fetch_collection_target_preview(id, first_page, ctx);
    }

    Ok(Some(IngestResult::Other(ThingLocalRef::CollectionTarget(
        id,
    ))))
}

async fn ingest_actorlike<
    K: activitystreams::base::AsBase
        + activitystreams::object::AsObject
        + activitystreams::markers::Actor
        + Clone
        + serde::Serialize,
>(
    actor: Verified<
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<K>,
            super::PublicKeyExtension<'static>,
        >,
    >,
    is_bot: bool,
    found_from: &FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let profile = super::target::classify_actor_value(&serde_json::to_value(&actor.0)?);
    let group_like_actor = actor_profile_is_group_like(&profile);

    if !group_like_actor {
        return ingest_personlike(actor, is_bot, ctx).await;
    }

    let ap_id = actor
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing ID in actor"))?;

    let name = actor
        .preferred_username()
        .or_else(|| {
            actor
                .name()
                .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()))
        })
        .unwrap_or("");
    let description_html = actor
        .summary()
        .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()));
    let inbox = actor.inbox_unchecked().as_str();
    let outbox = actor.outbox_unchecked();
    let followers = actor.followers_unchecked().map(|x| x.as_str());
    let shared_inbox = actor
        .endpoints_unchecked()
        .and_then(|endpoints| endpoints.shared_inbox.as_ref())
        .map(|url| url.as_str());
    let public_key = actor
        .ext_one
        .public_key
        .as_ref()
        .map(|key| key.public_key_pem.as_bytes());
    let public_key_sigalg = actor
        .ext_one
        .public_key
        .as_ref()
        .and_then(|key| key.signature_algorithm.as_deref());

    ingest_remote_community_actor(
        RemoteCommunityActor {
            ap_id,
            name,
            description_html,
            inbox,
            outbox,
            followers,
            shared_inbox,
            public_key,
            public_key_sigalg,
            featured: None,
        },
        found_from,
        ctx,
    )
    .await
}

fn actor_profile_is_group_like(profile: &super::target::TargetProfile) -> bool {
    /*
        Some useful group-like targets are not ActivityStreams Group actors.
        Service and Application actors can be relay groups, and blog publishers
        can be Person actors that own a public post stream. Those are stored as
        remote communities so the rest of lotide can follow and preview them.
    */
    matches!(
        profile.family,
        super::target::GroupTargetFamily::CollectionChannel
            | super::target::GroupTargetFamily::RelayBot
            | super::target::GroupTargetFamily::BlogPublisher
    ) && (profile.actor_kind != super::target::TargetActorKind::Person
        || profile.family == super::target::GroupTargetFamily::BlogPublisher)
}

async fn ingest_personlike<
    K: activitystreams::base::AsBase
        + activitystreams::object::AsObject
        + activitystreams::markers::Actor
        + Clone,
>(
    person: Verified<
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<K>,
            super::PublicKeyExtension<'static>,
        >,
    >,
    is_bot: bool,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let ap_id = person
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing ID in Person"))?;

    let username = person
        .preferred_username()
        .or_else(|| {
            person
                .name()
                .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()))
        })
        .unwrap_or("");
    let inbox = person.inbox_unchecked().as_str();
    let shared_inbox = person
        .endpoints_unchecked()
        .and_then(|endpoints| endpoints.shared_inbox.as_ref())
        .map(|url| url.as_str());
    let public_key = person
        .ext_one
        .public_key
        .as_ref()
        .map(|key| key.public_key_pem.as_bytes());
    let public_key_sigalg = person
        .ext_one
        .public_key
        .as_ref()
        .and_then(|key| key.signature_algorithm.as_deref());
    let description_html = person
        .summary()
        .and_then(|maybe| maybe.iter().find_map(|x| x.as_xsd_string()));

    let avatar = person.icon().and_then(|icon| {
        icon.iter().find_map(|icon| {
            if icon.kind_str() == Some("Image") {
                match activitystreams::object::Image::from_any_base(icon.clone()) {
                    Err(_) | Ok(None) => None,
                    Ok(Some(icon)) => Some(icon),
                }
            } else {
                None
            }
        })
    });
    let avatar = avatar
        .as_ref()
        .and_then(|icon| icon.url().and_then(|url| url.as_single_id()))
        .map(|x| x.as_str());

    let db = ctx.db_pool.get().await?;

    let id = UserLocalID(db.query_one(
        "INSERT INTO person (username, local, created_local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, avatar, is_bot) VALUES ($1, FALSE, localtimestamp, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description_html=$7, avatar=$8, is_bot=$9 RETURNING id",
        &[&username, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description_html, &avatar, &is_bot],
    ).await?.get(0));

    Ok(Some(IngestResult::Actor(super::ActorLocalInfo::User {
        id,
        public_key: public_key.map(|key| super::PubKeyInfo {
            algorithm: super::get_message_digest(public_key_sigalg),
            key: key.to_owned(),
        }),
        remote_url: super::url_from_ap_id(ap_id)?,
    })))
}

async fn handle_recieved_reply(
    obj: Verified<KnownObject>,
    object_id: &activitystreams::iri_string::types::IriString,
    content: &str,
    media_type: Option<&mime::Mime>,
    created: Option<&activitystreams::time::OffsetDateTime>,
    author: Option<&activitystreams::iri_string::types::IriString>,
    in_reply_to: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
    attachment_href: Option<&str>,
    sensitive: Option<bool>,
    mentions: Vec<crate::MentionInfo>,
    found_from: &FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<CommentLocalID>, crate::Error> {
    let mut db = ctx.db_pool.get().await?;

    let author = get_or_fetch_postlike_author_local_id(
        author,
        found_from
            .community()
            .map(|(community_local_id, _)| community_local_id),
        &db,
        &ctx,
    )
    .await?;
    let created = created.map(super::offset_datetime_to_chrono);

    let last_reply_to = in_reply_to.iter().last(); // TODO maybe not this? Not sure how to interpret inReplyTo

    if let Some(last_reply_to) = last_reply_to {
        if let Some(term_ap_id) = last_reply_to.as_xsd_any_uri() {
            #[derive(Debug)]
            enum ReplyTarget {
                Post {
                    id: PostLocalID,
                },
                Comment {
                    id: CommentLocalID,
                    post: PostLocalID,
                },
            }

            let target_is_remote =
                super::LocalObjectRef::try_from_uri(term_ap_id, &ctx.host_url_apub).is_none();
            let mut target = if let Some(local_id) =
                super::LocalObjectRef::try_from_uri(&term_ap_id, &ctx.host_url_apub)
            {
                match local_id {
                    super::LocalObjectRef::Post(post_id) => Some(ReplyTarget::Post { id: post_id }),
                    super::LocalObjectRef::Comment(local_comment_id) => {
                        let row = db
                            .query_opt("SELECT post FROM reply WHERE id=$1", &[&local_comment_id])
                            .await?;
                        row.map(|row| ReplyTarget::Comment {
                            id: local_comment_id,
                            post: PostLocalID(row.get(0)),
                        })
                    }
                    _ => None,
                }
            } else {
                let row = db
                    .query_opt("(SELECT id, post FROM reply WHERE ap_id=$1) UNION (SELECT NULL, id FROM post WHERE ap_id=$1) LIMIT 1", &[&term_ap_id.as_str()])
                    .await?;
                row.map(|row| match row.get::<_, Option<_>>(0).map(CommentLocalID) {
                    Some(reply_id) => ReplyTarget::Comment {
                        id: reply_id,
                        post: PostLocalID(row.get(1)),
                    },
                    None => ReplyTarget::Post {
                        id: PostLocalID(row.get(1)),
                    },
                })
            };

            if target.is_none() && target_is_remote {
                match crate::apub_util::fetch_and_ingest(
                    term_ap_id,
                    reply_parent_fetch_found_from(found_from),
                    ctx.clone(),
                )
                .await
                {
                    Ok(Some(result)) => {
                        target = match result.into_ref() {
                            ThingLocalRef::Post(id) => Some(ReplyTarget::Post { id }),
                            ThingLocalRef::Comment(id) => {
                                let row = db
                                    .query_opt("SELECT post FROM reply WHERE id=$1", &[&id])
                                    .await?;

                                row.map(|row| ReplyTarget::Comment {
                                    id,
                                    post: PostLocalID(row.get(0)),
                                })
                            }
                            _ => None,
                        };
                    }
                    Ok(None) => {}
                    Err(err) => {
                        log::warn!(
                            "Failed to fetch reply parent {term_ap_id} while ingesting {object_id}: {err:?}"
                        );
                    }
                }
            }

            if let Some(target) = target {
                let (post, parent) = match target {
                    ReplyTarget::Post { id } => (id, None),
                    ReplyTarget::Comment { id, post } => (post, Some(id)),
                };

                let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
                let (content_text, content_html) = if content_is_html {
                    (None, Some(content))
                } else {
                    (Some(content), None)
                };

                let sensitive = sensitive.unwrap_or(false);

                let row = {
                    let trans = db.transaction().await?;
                    let row = trans.query_opt(
                        "INSERT INTO reply (post, parent, author, content_text, content_html, created, local, ap_id, attachment_href, sensitive) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), FALSE, $7, $8, $9) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
                        &[&post, &parent, &author, &content_text, &content_html, &created, &object_id.as_str(), &attachment_href, &sensitive],
                    ).await?;

                    if row.is_none() && author.is_some() {
                        trans
                            .execute(
                                "UPDATE reply SET author=COALESCE(author, $1) WHERE ap_id=$2",
                                &[&author, &object_id.as_str()],
                            )
                            .await?;
                    }

                    if let Some(row) = &row {
                        let id = CommentLocalID(row.get(0));

                        if !mentions.is_empty() {
                            let (nest_person, nest_text): (Vec<_>, Vec<_>) = mentions
                                .iter()
                                .map(|info| (info.person, &info.text))
                                .unzip();

                            trans.execute(
                                "INSERT INTO reply_mention (reply, person, text) SELECT $1, * FROM UNNEST($2::BIGINT[], $3::TEXT[]) ON CONFLICT DO NOTHING",
                                &[&id, &nest_person, &nest_text],
                            ).await?;
                        }
                    }

                    trans.commit().await?;

                    row
                };

                if let Some(row) = row {
                    let id = CommentLocalID(row.get(0));
                    let info = crate::CommentInfo {
                        id,
                        author,
                        post,
                        parent,
                        content_text: content_text.map(|x| Cow::Owned(x.to_owned())),
                        content_markdown: None,
                        content_html: content_html.map(|x| Cow::Owned(x.to_owned())),
                        created: created.unwrap_or_else(|| {
                            chrono::offset::Utc::now().with_timezone(&UTC_OFFSET)
                        }),
                        ap_id: crate::APIDOrLocal::APID(super::url_from_ap_id(object_id)?),
                        attachment_href: attachment_href.map(|x| Cow::Owned(x.to_owned())),
                        sensitive,
                        mentions: Cow::Owned(mentions),
                    };

                    crate::on_post_add_comment(info, ctx.clone());

                    crate::spawn_task(async move {
                        // if this is in a local community, we need to forward it to followers

                        let row = db.query_opt(
                            "SELECT id FROM community WHERE local AND id = (SELECT community FROM post WHERE id=$1)",
                            &[&post],
                        ).await?;

                        if let Some(row) = row {
                            crate::apub_util::enqueue_forward_to_community_followers(
                                CommunityLocalID(row.get(0)),
                                serde_json::to_string(&obj)?,
                                ctx.clone(),
                            )
                            .await?;
                        }

                        Ok(())
                    });

                    Ok(Some(id))
                } else {
                    // not new, try to fetch id
                    // will probably be unnecessary when we implement comment editing

                    let row = db
                        .query_opt(
                            "SELECT id FROM reply WHERE ap_id=$1",
                            &[&object_id.as_str()],
                        )
                        .await?;
                    Ok(row.map(|row| CommentLocalID(row.get(0))))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

async fn enqueue_local_unfollows_for_untracked_remote_community(
    community_local_id: CommunityLocalID,
    ctx: Arc<crate::RouteContext>,
) -> Result<usize, crate::Error> {
    let mut db = ctx.db_pool.get().await?;
    let new_undos = {
        let trans = db.transaction().await?;
        let rows = trans
            .query(
                DELETE_LOCAL_COMMUNITY_FOLLOWS_FOR_UNTRACKED_SQL,
                &[&community_local_id],
            )
            .await?;
        let mut new_undos = Vec::with_capacity(rows.len());

        for row in rows {
            let follower = UserLocalID(row.get(0));
            let follow_ap_id: Option<&str> = row.get(1);
            let id = uuid::Uuid::new_v4();

            trans
                .execute(
                    "INSERT INTO local_community_follow_undo (id, community, follower, follow_ap_id) VALUES ($1, $2, $3, $4)",
                    &[&id, &community_local_id, &follower, &follow_ap_id],
                )
                .await?;

            new_undos.push((id, follower));
        }

        trans.commit().await?;

        new_undos
    };

    for (undo, follower) in &new_undos {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(
            *undo,
            community_local_id,
            *follower,
            ctx.clone(),
        );
    }

    Ok(new_undos.len())
}

async fn remote_community_post_is_wanted(
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    allow_untracked_remote_community: bool,
    ctx: Arc<crate::RouteContext>,
) -> Result<bool, crate::Error> {
    if community_is_local {
        return Ok(true);
    }

    let db = ctx.db_pool.get().await?;
    let row = db
        .query_opt(REMOTE_COMMUNITY_TRACKING_SQL, &[&community_local_id])
        .await?
        .map(|row| (row.get::<_, bool>(0), row.get::<_, bool>(1)));
    drop(db);

    let Some((community_deleted, has_local_follow)) = row else {
        log::debug!("refusing to ingest object for missing remote community {community_local_id}");
        return Ok(false);
    };

    if community_deleted {
        log::debug!("refusing to ingest object for deleted remote community {community_local_id}");
        return Ok(false);
    }

    if has_local_follow || allow_untracked_remote_community {
        Ok(true)
    } else {
        let undo_count =
            enqueue_local_unfollows_for_untracked_remote_community(community_local_id, ctx.clone())
                .await?;
        let db = ctx.db_pool.get().await?;
        let deleted_empty_community = db
            .execute(
                DELETE_EMPTY_UNTRACKED_REMOTE_COMMUNITY_SQL,
                &[&community_local_id],
            )
            .await?;

        log::debug!(
            "refusing to ingest object for unfollowed remote community {community_local_id}; enqueued {undo_count} local follow undo tasks; deleted {deleted_empty_community} empty community rows"
        );

        Ok(false)
    }
}

async fn handle_received_page_for_community<Kind: Clone + std::fmt::Debug>(
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    allow_untracked_remote_community: bool,
    approved: bool,
    approved_ap_id: Option<&str>,
    poll_info: Option<PollIngestInfo>,
    mentions: Vec<crate::MentionInfo>,
    href: Option<String>,
    obj: Verified<ExtendedPostlike<activitystreams::object::Object<Kind>>>,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<PostIngestResult>, crate::Error> {
    if !remote_community_post_is_wanted(
        community_local_id,
        community_is_local,
        allow_untracked_remote_community,
        ctx.clone(),
    )
    .await?
    {
        return Ok(None);
    }

    let title = obj
        .name()
        .iter()
        .chain(obj.summary().iter())
        .map(|x| x.iter())
        .flatten()
        .find_map(|maybe| maybe.as_xsd_string())
        .unwrap_or("");
    let content = obj.content();
    let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
    let media_type = obj.media_type();
    let created = obj.published();
    let author = postlike_author_id(obj.attributed_to());
    let sensitive = obj.ext_two.sensitive;
    let mbin_source_id = obj.ext_three.lotide_mbin_source_id.as_ref();

    if let Some(object_id) = obj.id_unchecked() {
        if let Some(author) = &author {
            require_containment_or_mbin_mirror_source(object_id, author, mbin_source_id)?;
        }

        Ok(Some(
            handle_recieved_post(
                object_id.clone(),
                title,
                href.as_deref(),
                content,
                media_type,
                created.as_ref(),
                author,
                community_local_id,
                community_is_local,
                approved,
                approved_ap_id,
                poll_info,
                sensitive,
                mentions,
                ctx,
            )
            .await?,
        ))
    } else {
        Ok(None)
    }
}

fn require_containment_or_mbin_mirror_source(
    object_id: &activitystreams::iri_string::types::IriString,
    author: &activitystreams::iri_string::types::IriString,
    mbin_source_id: Option<&url::Url>,
) -> Result<(), super::NotContained> {
    if super::is_contained(object_id, author) {
        return Ok(());
    }

    /*
        Mbin can expose a mirrored entry using the Mbin-local entry URL while
        also reporting the original source object and source author. Keep the
        normal containment rule strict, but accept this API-normalized shape
        when the source object shares the author's origin.
    */
    if mbin_source_id.is_some_and(|source_id| super::is_contained(source_id, author)) {
        return Ok(());
    }

    Err(super::NotContained)
}

async fn handle_recieved_post(
    object_id: activitystreams::iri_string::types::IriString,
    title: &str,
    href: Option<&str>,
    content: Option<&str>,
    media_type: Option<&mime::Mime>,
    created: Option<&activitystreams::time::OffsetDateTime>,
    author_ap_id: Option<activitystreams::iri_string::types::IriString>,
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    approved: bool,
    approved_ap_id: Option<&str>,
    poll_info: Option<PollIngestInfo>,
    sensitive: Option<bool>,
    mentions: Vec<crate::MentionInfo>,
    ctx: Arc<crate::RouteContext>,
) -> Result<PostIngestResult, crate::Error> {
    let mut db = ctx.db_pool.get().await?;
    let author = get_or_fetch_postlike_author_local_id(
        author_ap_id.as_ref(),
        Some(community_local_id),
        &db,
        &ctx,
    )
    .await?;

    let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
    let (content_text, content_html) = if content_is_html {
        (None, Some(content.unwrap_or("")))
    } else {
        (Some(content.unwrap_or("")), None)
    };
    let title = crate::post_title_or_fallback(title, content_text, None, content_html);

    let approved = approved || community_is_local;
    let created = created.map(super::offset_datetime_to_chrono);

    let sensitive = sensitive.unwrap_or(false);

    let (post_local_id, poll_output, created, is_new) = {
        let trans = db.transaction().await?;
        let row = trans.query_one(
            "INSERT INTO post (author, href, content_text, content_html, title, created, community, local, ap_id, approved, approved_ap_id, updated_local, sensitive) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), $7, FALSE, $8, $9, $10, current_timestamp, $11) ON CONFLICT (ap_id) DO UPDATE SET author=COALESCE(post.author, $1), approved=($9 OR post.approved), approved_ap_id=(CASE WHEN $9 THEN $10 ELSE post.approved_ap_id END), updated_local=current_timestamp, sensitive=$11 RETURNING id, poll_id, created, (xmax = 0)",
            &[&author, &href, &content_text, &content_html, &title, &created, &community_local_id, &object_id.as_str(), &approved, &approved_ap_id, &sensitive],
        ).await?;
        let post_local_id = PostLocalID(row.get(0));
        let existing_poll_id: Option<i64> = row.get(1);
        let created = row.get(2);
        let is_new = row.get(3);

        if !mentions.is_empty() {
            log::debug!("inserting mentions {mentions:?}");
            let (nest_person, nest_text): (Vec<_>, Vec<_>) = mentions
                .iter()
                .map(|info| (info.person, &info.text))
                .unzip();

            trans.execute(
                "INSERT INTO post_mention (post, person, text) SELECT $1, * FROM UNNEST($2::BIGINT[], $3::TEXT[]) ON CONFLICT DO NOTHING",
                &[&post_local_id, &nest_person, &nest_text],
            ).await?;
        }

        let poll_output = if let Some(poll_id) = existing_poll_id {
            if let Some(poll_info) = &poll_info {
                let names: Vec<&str> = poll_info.options.iter().map(|(name, _)| &**name).collect();
                let counts: Vec<Option<i32>> = poll_info
                    .options
                    .iter()
                    .map(|(_, count)| count.clone())
                    .collect();
                let option_count = crate::usize_to_i32(poll_info.options.len())?;
                let indices: Vec<i32> = (0..option_count).collect();

                let is_closed: bool = trans
                    .query_one(
                        "UPDATE poll SET multiple=$1, is_closed=$3, closed_at=$4 WHERE id=$2 RETURNING COALESCE(is_closed, closed_at < current_timestamp, FALSE)",
                        &[
                            &poll_info.multiple,
                            &poll_id,
                            &poll_info.is_closed,
                            &poll_info.closed_at,
                        ],
                    )
                    .await?
                    .get(0);
                trans
                    .execute(
                        "DELETE FROM poll_option WHERE poll_id=$1 AND NOT (name = ANY($2::TEXT[]))",
                        &[&poll_id, &names],
                    )
                    .await?;

                let options_rows = trans.query("INSERT INTO poll_option (poll_id, name, position, remote_vote_count) SELECT $1, * FROM UNNEST($2::TEXT[], $3::INTEGER[], $4::INTEGER[]) ON CONFLICT (poll_id, name) DO UPDATE SET position = excluded.position, remote_vote_count = excluded.remote_vote_count RETURNING id, position", &[&poll_id, &names, &indices, &counts]).await?;

                let mut options: Vec<_> = options_rows
                    .into_iter()
                    .map(|row| (PollOptionLocalID(row.get(0)), row.get::<_, i32>(1)))
                    .collect();
                options.sort_unstable_by_key(|x| x.1);

                Some((options, is_closed))
            } else {
                trans
                    .execute(
                        "UPDATE post SET poll_id=NULL WHERE id=$1",
                        &[&post_local_id],
                    )
                    .await?;
                trans
                    .execute("DELETE FROM poll WHERE id=$1", &[&poll_id])
                    .await?;

                None
            }
        } else {
            if let Some(poll_info) = &poll_info {
                let names: Vec<&str> = poll_info.options.iter().map(|(name, _)| &**name).collect();
                let counts: Vec<Option<i32>> = poll_info
                    .options
                    .iter()
                    .map(|(_, count)| count.clone())
                    .collect();
                let option_count = crate::usize_to_i32(poll_info.options.len())?;
                let indices: Vec<i32> = (0..option_count).collect();

                let row = trans
                    .query_one(
                        "INSERT INTO poll (multiple, is_closed, closed_at) VALUES ($1, $2, $3) RETURNING id, COALESCE(is_closed, closed_at < current_timestamp, FALSE)",
                        &[&poll_info.multiple, &poll_info.is_closed, &poll_info.closed_at],
                    )
                    .await?;
                let poll_id: i64 = row.get(0);
                let is_closed: bool = row.get(1);

                let options_rows = trans.query("INSERT INTO poll_option (poll_id, name, position, remote_vote_count) SELECT $1, * FROM UNNEST($2::TEXT[], $3::INTEGER[], $4::INTEGER[]) RETURNING id, position", &[&poll_id, &names, &indices, &counts]).await?;
                trans
                    .execute(
                        "UPDATE post SET poll_id=$1 WHERE id=$2",
                        &[&poll_id, &post_local_id],
                    )
                    .await?;

                let mut options: Vec<_> = options_rows
                    .into_iter()
                    .map(|row| (PollOptionLocalID(row.get(0)), row.get::<_, i32>(1)))
                    .collect();
                options.sort_unstable_by_key(|x| x.1);

                Some((options, is_closed))
            } else {
                None
            }
        };

        trans.commit().await?;

        (post_local_id, poll_output, created, is_new)
    };

    let poll = poll_output.map(|(options, is_closed)| {
        let info = poll_info.unwrap();

        crate::PollInfoOwned {
            multiple: info.multiple,
            options: options
                .into_iter()
                .zip(info.options)
                .map(|((id, _), (name, votes))| crate::PollOptionOwned {
                    id,
                    name,
                    votes: crate::i32_to_u32_saturating(votes.unwrap_or(0)),
                })
                .collect(),
            is_closed,
            closed_at: info.closed_at,
        }
    });

    let object_url = super::url_from_ap_id(&object_id)?;
    let author_url = author_ap_id
        .as_ref()
        .map(super::url_from_ap_id)
        .transpose()?;

    let post = crate::PostInfoOwned {
        id: post_local_id,
        ap_id: crate::APIDOrLocal::APID(object_url),
        author_ap_id: author_url.map(crate::APIDOrLocal::APID),
        author,
        href: href.map(std::borrow::ToOwned::to_owned),
        content_text: content_text.map(std::borrow::ToOwned::to_owned),
        content_markdown: None,
        content_html: content_html.map(std::borrow::ToOwned::to_owned),
        title,
        created,
        community: community_local_id,
        poll: poll.clone(),
        sensitive,
        mentions,
    };

    crate::on_add_post(post, community_is_local, is_new, ctx);

    Ok(PostIngestResult {
        id: post_local_id,
        poll: poll.map(std::convert::Into::into),
    })
}

fn try_transform_inner<T: TryInto<U>, U>(
    l: ExtendedPostlike<T>,
) -> Result<ExtendedPostlike<U>, T::Error> {
    Ok(ExtendedPostlike::new(
        l.inner.try_into()?,
        l.ext_one,
        l.ext_two,
        l.ext_three,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn any_base(value: serde_json::Value) -> activitystreams::base::AnyBase {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn mbin_mirror_source_can_validate_cross_origin_author() {
        let object_id = "https://thebrainbin.org/m/AskMbin/t/1678740"
            .parse::<activitystreams::iri_string::types::IriString>()
            .unwrap();
        let source_id = "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
            .parse::<url::Url>()
            .unwrap();
        let author = "https://feddit.online/u/MastKalandar"
            .parse::<activitystreams::iri_string::types::IriString>()
            .unwrap();

        assert!(
            super::require_containment_or_mbin_mirror_source(&object_id, &author, Some(&source_id))
                .is_ok()
        );
    }

    #[test]
    fn mbin_mirror_source_does_not_validate_unrelated_author() {
        let object_id = "https://thebrainbin.org/m/AskMbin/t/1678740"
            .parse::<activitystreams::iri_string::types::IriString>()
            .unwrap();
        let source_id = "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
            .parse::<url::Url>()
            .unwrap();
        let author = "https://spoof.example/u/not-the-author"
            .parse::<activitystreams::iri_string::types::IriString>()
            .unwrap();

        assert!(
            super::require_containment_or_mbin_mirror_source(&object_id, &author, Some(&source_id))
                .is_err()
        );
    }

    #[test]
    fn postlike_page_transform_preserves_mbin_source_extension() {
        let source_id = "https://feddit.online/c/AskMbin/p/1716853/ls-it-possible-to-experiment-with-dns-on-a-virtual-machine"
            .parse::<url::Url>()
            .unwrap();
        let mut page =
            crate::apub_util::make_extended_postlike(activitystreams::object::Page::new());

        page.ext_three.lotide_mbin_source_id = Some(source_id.clone());

        let transformed: crate::apub_util::ExtendedPostlike<activitystreams::object::Page> =
            super::try_transform_inner(page).unwrap();

        assert_eq!(
            transformed.ext_three.lotide_mbin_source_id.as_ref(),
            Some(&source_id)
        );
    }

    #[test]
    fn known_object_accepts_lemmy_page_with_link_attachment() {
        let object: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": "https://lemmy.example/post/1",
            "type": "Page",
            "name": "Lemmy post",
            "attributedTo": "https://lemmy.example/u/alice",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "attachment": [{
                "type": "Link",
                "href": "https://lemmy.example/pictrs/image/a.png"
            }]
        }))
        .unwrap();

        assert!(matches!(object, crate::apub_util::KnownObject::Page(_)));
    }

    #[test]
    fn known_object_accepts_peertube_video() {
        let object = crate::apub_util::deserialize_known_object_value(serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": "https://spectra.video/videos/watch/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac",
            "type": "Video",
            "name": "FediForum Talk: Atproto: A Technical Introduction",
            "published": "2026-05-04T22:23:20.204Z",
            "attributedTo": [
                {
                    "type": "Person",
                    "id": "https://spectra.video/accounts/fediforum"
                },
                {
                    "type": "Group",
                    "id": "https://spectra.video/video-channels/fediforum_demos"
                }
            ],
            "to": [
                "https://www.w3.org/ns/activitystreams#Public",
                "https://spectra.video/video-channels/fediforum_demos"
            ],
            "cc": ["https://spectra.video/accounts/fediforum/followers"],
            "url": [
                {
                    "type": "Link",
                    "mediaType": "text/html",
                    "href": "https://spectra.video/w/mDnE8FbYh1PXiaCPxsniGQ"
                },
                {
                    "type": "Link",
                    "mediaType": "video/mp4",
                    "href": "https://spectra.example/video.mp4"
                },
                {
                    "type": "Link",
                    "rel": ["metadata", "video/mp4"],
                    "mediaType": "application/json",
                    "href": "https://spectra.video/api/v1/videos/a72ea3ba-ddcd-40f6-af9f-8219b72bd6ac/metadata/1022722"
                },
                {
                    "type": "Link",
                    "mediaType": "application/x-bittorrent;x-scheme-handler/magnet",
                    "href": "magnet:?xt=urn:btih:77cac1a953b03228ad10c7971fc694fa91232903"
                },
                {
                    "type": "Link",
                    "mediaType": "application/x-mpegURL",
                    "href": "https://spectra.example/master.m3u8",
                    "tag": [
                        {
                            "type": "Infohash",
                            "name": "5158374f465361685a39786b5034675457597a59"
                        }
                    ]
                }
            ],
            "content": "By Daniel Holmgren, head of protocol at Bluesky PBC.",
            "mediaType": "text/markdown"
        }))
        .unwrap();

        let video = match object {
            crate::apub_util::KnownObject::Video(video) => video,
            _ => panic!("expected Video object"),
        };

        assert_eq!(
            postlike_author_id(video.attributed_to())
                .as_ref()
                .map(|id| id.as_str()),
            Some("https://spectra.video/accounts/fediforum")
        );
    }

    #[test]
    fn known_object_accepts_application_actor() {
        let object = crate::apub_util::deserialize_known_object_value(serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Application",
            "id": "https://gancio.example/federation/u/events",
            "preferredUsername": "events",
            "inbox": "https://gancio.example/federation/u/events/inbox",
            "outbox": "https://gancio.example/federation/u/events/outbox"
        }))
        .unwrap();

        assert!(matches!(
            object,
            crate::apub_util::KnownObject::Application(_)
        ));
    }

    #[test]
    fn known_object_accepts_media_and_event_postlikes() {
        for kind in ["Audio", "Document", "Event", "Video"] {
            let object: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
                "@context": "https://www.w3.org/ns/activitystreams",
                "id": format!("https://remote.example/objects/{}", kind.to_lowercase()),
                "type": kind,
                "name": format!("{} object", kind),
                "attributedTo": "https://remote.example/users/alice",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "url": format!("https://remote.example/media/{}", kind.to_lowercase())
            }))
            .unwrap();

            match (kind, object) {
                ("Audio", crate::apub_util::KnownObject::Audio(_)) => {}
                ("Document", crate::apub_util::KnownObject::Document(_)) => {}
                ("Event", crate::apub_util::KnownObject::Event(_)) => {}
                ("Video", crate::apub_util::KnownObject::Video(_)) => {}
                _ => panic!("unexpected object type for {}", kind),
            }
        }
    }

    #[test]
    fn attachment_href_accepts_link_href() {
        let attachment = any_base(serde_json::json!({
            "type": "Link",
            "href": "https://lemmy.example/pictrs/image/a.png"
        }));

        assert_eq!(
            get_attachment_href(&attachment).unwrap().as_deref(),
            Some("https://lemmy.example/pictrs/image/a.png")
        );
    }

    #[test]
    fn attachment_href_accepts_nested_image_url() {
        let attachment = any_base(serde_json::json!({
            "type": "Image",
            "url": {
                "type": "Link",
                "href": "https://piefed.example/media/post-image.webp"
            }
        }));

        assert_eq!(
            get_attachment_href(&attachment).unwrap().as_deref(),
            Some("https://piefed.example/media/post-image.webp")
        );
    }

    #[test]
    fn blank_post_titles_use_body_or_no_title() {
        assert_eq!(
            crate::post_title_or_fallback("   ", Some("First line\nSecond line"), None, None,),
            "First line"
        );

        assert_eq!(
            crate::post_title_or_fallback("   ", None, None, Some("<p>Body title</p>")),
            "Body title"
        );

        assert_eq!(
            crate::post_title_or_fallback("   ", None, None, None),
            "[no title]"
        );
    }

    #[test]
    fn remote_community_ingest_checks_accepted_local_follows() {
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("deleted"));
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("community_follow"));
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("community_follow.community=community.id"));
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("community_follow.local"));
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("community_follow.accepted"));
        assert!(REMOTE_COMMUNITY_TRACKING_SQL.contains("FROM community WHERE id=$1"));
    }

    #[test]
    fn postlike_author_resolution_allows_the_community_actor() {
        assert!(POSTLIKE_AUTHOR_IS_COMMUNITY_SQL.contains("FROM community"));
        assert!(POSTLIKE_AUTHOR_IS_COMMUNITY_SQL.contains("id=$1"));
        assert!(POSTLIKE_AUTHOR_IS_COMMUNITY_SQL.contains("ap_id=$2"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("INSERT INTO person"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("FROM community"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("AND NOT local"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("AND NOT deleted"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("ap_id=$2"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("ON CONFLICT (ap_id) DO UPDATE"));
        assert!(UPSERT_REMOTE_COMMUNITY_AUTHOR_SQL.contains("RETURNING id"));
    }

    #[test]
    fn untracked_remote_community_cleanup_only_deletes_empty_remote_rows() {
        let sql = DELETE_EMPTY_UNTRACKED_REMOTE_COMMUNITY_SQL;

        assert!(sql.contains("DELETE FROM community"));
        assert!(sql.contains("WHERE id=$1"));
        assert!(sql.contains("AND NOT local"));
        assert!(sql.contains("AND NOT deleted"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("community_follow.community=community.id"));
        assert!(sql.contains("post.community=community.id"));
    }

    #[test]
    fn explicit_lookup_keeps_untracked_remote_groups() {
        assert!(FoundFrom::ExplicitLookup.keeps_untracked_remote_group());
        assert!(FoundFrom::ExplicitLookup.allows_untracked_remote_community());
        assert!(!FoundFrom::Other.keeps_untracked_remote_group());
        assert!(!FoundFrom::Refresh.keeps_untracked_remote_group());
    }

    #[test]
    fn wordpress_person_actor_is_group_like_for_blog_ingest() {
        let profile = crate::apub_util::target::TargetProfile {
            target: crate::apub_util::target::GroupTarget::WordPress,
            family: crate::apub_util::target::GroupTargetFamily::BlogPublisher,
            actor_kind: crate::apub_util::target::TargetActorKind::Person,
            has_inbox: true,
            has_outbox: true,
            has_followers: true,
            has_featured: false,
        };

        assert!(actor_profile_is_group_like(&profile));
    }

    #[test]
    fn ordinary_person_actor_stays_user_like() {
        let profile = crate::apub_util::target::TargetProfile {
            target: crate::apub_util::target::GroupTarget::Mastodon,
            family: crate::apub_util::target::GroupTargetFamily::ProfileOnly,
            actor_kind: crate::apub_util::target::TargetActorKind::Person,
            has_inbox: true,
            has_outbox: true,
            has_followers: true,
            has_featured: false,
        };

        assert!(!actor_profile_is_group_like(&profile));
    }

    #[test]
    fn announce_keeps_outbox_preview_untracked_allowance() {
        let outbox_preview = FoundFrom::CommunityOutbox {
            community_local_id: CommunityLocalID(1),
            community_is_local: false,
            preview: true,
        };
        let outbox_routine = FoundFrom::CommunityOutbox {
            community_local_id: CommunityLocalID(1),
            community_is_local: false,
            preview: false,
        };
        let announce = FoundFrom::Announce {
            url: "https://example.com/activity/1".parse().unwrap(),
            community_local_id: CommunityLocalID(1),
            community_is_local: false,
            allow_untracked_remote_community: outbox_preview.allows_untracked_remote_community(),
        };

        assert!(outbox_preview.allows_untracked_remote_community());
        assert!(announce.allows_untracked_remote_community());
        assert!(!outbox_routine.allows_untracked_remote_community());
    }

    #[test]
    fn reply_parent_fetch_keeps_approved_community_context() {
        let outbox_preview = FoundFrom::CommunityOutbox {
            community_local_id: CommunityLocalID(1),
            community_is_local: false,
            preview: true,
        };
        let announce = FoundFrom::Announce {
            url: "https://remote.example/activity/announce".parse().unwrap(),
            community_local_id: CommunityLocalID(2),
            community_is_local: false,
            allow_untracked_remote_community: false,
        };

        assert!(matches!(
            reply_parent_fetch_found_from(&outbox_preview),
            FoundFrom::CommunityOutbox {
                community_local_id: CommunityLocalID(1),
                community_is_local: false,
                preview: true
            }
        ));
        assert!(matches!(
            reply_parent_fetch_found_from(&announce),
            FoundFrom::Announce {
                community_local_id: CommunityLocalID(2),
                community_is_local: false,
                allow_untracked_remote_community: false,
                ..
            }
        ));
        assert!(matches!(
            reply_parent_fetch_found_from(&FoundFrom::Other),
            FoundFrom::Refresh
        ));
        assert!(matches!(
            reply_parent_fetch_found_from(&FoundFrom::Refresh),
            FoundFrom::Refresh
        ));
    }

    #[test]
    fn announce_wrapped_local_likes_are_confirmable() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let announce: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "@context": [
                "https://join-lemmy.org/context.json",
                "https://www.w3.org/ns/activitystreams"
            ],
            "type": "Announce",
            "id": "https://ani.example/activities/announce/like/1",
            "actor": "https://ani.example/c/animemes",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
            "cc": ["https://ani.example/c/animemes/followers"],
            "object": {
                "type": "Like",
                "id": "https://lotide.example/apub/posts/572789/likes/1?activity=c6ee592d-bb5a-4b90-9f2a-175b5747b5fd",
                "actor": "https://lotide.example/apub/users/1",
                "object": "https://lemmy.example/post/43780773",
                "audience": "https://ani.example/c/animemes",
                "to": [
                    "https://www.w3.org/ns/activitystreams#Public",
                    "https://lemmy.example/u/author"
                ],
                "cc": ["https://ani.example/c/animemes"]
            }
        }))
        .unwrap();

        let announce = match announce {
            crate::apub_util::KnownObject::Announce(announce) => announce,
            _ => panic!("expected Announce object"),
        };
        let (_actor, object, _target, _activity) = announce.into_parts();

        assert_eq!(
            local_announced_object_id(&object, &host_url_apub)
                .as_ref()
                .map(|id| id.as_str()),
            Some(
                "https://lotide.example/apub/posts/572789/likes/1?activity=c6ee592d-bb5a-4b90-9f2a-175b5747b5fd"
            )
        );
    }

    #[test]
    fn announce_wrapped_remote_likes_still_need_normal_ingest() {
        let host_url_apub = "https://lotide.example/apub".parse().unwrap();
        let announce: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Announce",
            "id": "https://ani.example/activities/announce/like/2",
            "actor": "https://ani.example/c/animemes",
            "object": {
                "type": "Like",
                "id": "https://ani.example/activities/like/2",
                "actor": "https://ani.example/u/alice",
                "object": "https://lotide.example/apub/posts/572789"
            }
        }))
        .unwrap();

        let announce = match announce {
            crate::apub_util::KnownObject::Announce(announce) => announce,
            _ => panic!("expected Announce object"),
        };
        let (_actor, object, _target, _activity) = announce.into_parts();

        assert!(local_announced_object_id(&object, &host_url_apub).is_none());
    }

    #[test]
    fn posted_like_confirmation_updates_full_status() {
        for sql in [
            MARK_LOCAL_POST_LIKE_POSTED_SQL,
            MARK_LOCAL_REPLY_LIKE_POSTED_SQL,
        ] {
            assert!(sql.contains("federation_sent_at"));
            assert!(sql.contains("federation_received_at"));
            assert!(sql.contains("federation_posted_at"));
            assert!(sql.contains("COALESCE"));
        }
    }

    #[test]
    fn embedded_create_trust_accepts_friendica_urn_prefix() {
        let object_id = "urn:X-dfrn:forum.friendi.ca:3:39bbe52a195b057d5c93d7f116771904"
            .parse()
            .unwrap();
        let activity_id = "urn:X-dfrn:forum.friendi.ca:3:39bbe52a195b057d5c93d7f116771904/Create"
            .parse()
            .unwrap();
        let unrelated_id = "urn:X-dfrn:forum.friendi.ca:3:other".parse().unwrap();

        assert!(embedded_activity_object_is_trusted(
            &activity_id,
            &object_id
        ));
        assert!(!embedded_activity_object_is_trusted(
            &activity_id,
            &unrelated_id
        ));
    }

    #[test]
    fn stale_remote_community_unfollow_keeps_real_actor_context() {
        assert!(DELETE_LOCAL_COMMUNITY_FOLLOWS_FOR_UNTRACKED_SQL.contains("community=$1"));
        assert!(DELETE_LOCAL_COMMUNITY_FOLLOWS_FOR_UNTRACKED_SQL.contains("local"));
        assert!(DELETE_LOCAL_COMMUNITY_FOLLOWS_FOR_UNTRACKED_SQL.contains("RETURNING follower"));
    }

    #[test]
    fn followlike_id_extracts_embedded_objects() {
        let follow: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Follow",
            "id": "https://mastodon.example/activities/1",
            "actor": {
                "type": "Person",
                "id": "https://mastodon.example/users/alice"
            },
            "object": {
                "type": "Group",
                "id": "https://lemmy.example/c/rust"
            }
        }))
        .unwrap();

        let follow = match follow {
            crate::apub_util::KnownObject::Follow(follow) => follow,
            _ => panic!("expected Follow object"),
        };

        assert_eq!(
            followlike_id(follow.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://mastodon.example/users/alice"
        );
        assert_eq!(
            followlike_id(follow.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://lemmy.example/c/rust"
        );
    }

    #[test]
    fn followlike_id_extracts_id_from_array() {
        let follow: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Follow",
            "id": "https://pleroma.example/activities/2",
            "actor": [
                {
                    "type": "Person",
                    "id": "https://pleroma.example/u/bob"
                }
            ],
            "object": [
                "https://community.example/c/sandbox"
            ]
        }))
        .unwrap();

        let follow = match follow {
            crate::apub_util::KnownObject::Follow(follow) => follow,
            _ => panic!("expected Follow object"),
        };

        assert_eq!(
            followlike_id(follow.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://pleroma.example/u/bob"
        );
        assert_eq!(
            followlike_id(follow.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://community.example/c/sandbox"
        );
    }

    #[test]
    fn accept_id_extracts_embedded_follow_object() {
        let accept: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Accept",
            "id": "https://community.example/activities/accept/1",
            "actor": {
                "type": "Group",
                "id": "https://community.example/c/sandbox"
            },
            "object": {
                "type": "Follow",
                "id": "https://lotide.example/apub/communities/10/followers/1",
                "actor": "https://lotide.example/apub/users/1",
                "object": "https://community.example/c/sandbox"
            }
        }))
        .unwrap();

        let accept = match accept {
            crate::apub_util::KnownObject::Accept(accept) => accept,
            _ => panic!("expected Accept object"),
        };

        assert_eq!(
            followlike_id(accept.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://community.example/c/sandbox"
        );
        assert_eq!(
            followlike_id(accept.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://lotide.example/apub/communities/10/followers/1"
        );
    }

    #[test]
    fn reject_id_extracts_embedded_follow_object() {
        let reject: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Reject",
            "id": "https://community.example/activities/reject/1",
            "actor": "https://community.example/c/sandbox",
            "object": {
                "type": "Follow",
                "id": "https://lotide.example/apub/communities/10/followers/1",
                "actor": "https://lotide.example/apub/users/1",
                "object": "https://community.example/c/sandbox"
            }
        }))
        .unwrap();

        let reject = match reject {
            crate::apub_util::KnownObject::Reject(reject) => reject,
            _ => panic!("expected Reject object"),
        };

        assert_eq!(
            followlike_id(reject.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://community.example/c/sandbox"
        );
        assert_eq!(
            followlike_id(reject.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://lotide.example/apub/communities/10/followers/1"
        );
    }

    #[test]
    fn like_id_extracts_embedded_actor_and_object() {
        let like: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Like",
            "id": "https://piefed.example/activities/like/1",
            "actor": [{
                "type": "Person",
                "id": "https://piefed.example/u/alice"
            }],
            "object": {
                "type": "Page",
                "id": "https://lotide.example/apub/posts/10"
            }
        }))
        .unwrap();

        let like = match like {
            crate::apub_util::KnownObject::Like(like) => like,
            _ => panic!("expected Like object"),
        };

        assert_eq!(
            followlike_id(like.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://piefed.example/u/alice"
        );
        assert_eq!(
            followlike_id(like.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://lotide.example/apub/posts/10"
        );
    }

    #[test]
    fn undo_id_extracts_embedded_like_object() {
        let undo: crate::apub_util::KnownObject = serde_json::from_value(serde_json::json!({
            "type": "Undo",
            "id": "https://mbin.example/activities/undo/1",
            "actor": {
                "type": "Person",
                "id": "https://mbin.example/u/alice"
            },
            "object": {
                "type": "Like",
                "id": "https://mbin.example/activities/like/1",
                "actor": "https://mbin.example/u/alice",
                "object": "https://lotide.example/apub/posts/10"
            }
        }))
        .unwrap();

        let undo = match undo {
            crate::apub_util::KnownObject::Undo(undo) => undo,
            _ => panic!("expected Undo object"),
        };

        assert_eq!(
            followlike_id(undo.actor_unchecked())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://mbin.example/u/alice"
        );
        assert_eq!(
            followlike_id(undo.object())
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or(""),
            "https://mbin.example/activities/like/1"
        );
    }
}
