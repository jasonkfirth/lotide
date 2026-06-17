/*
    Project: Lotide ActivityPub Compatibility
    -----------------------------------------

    File: target.rs

    Purpose:

        Describe the remote ActivityPub group targets that lotide knows how
        to reason about.

    Responsibilities:

        - group remote software into protocol families
        - record the operations lotide should test for each target
        - provide light-weight actor JSON classification
        - provide shared actor path hints for handle fallback lookup

    This file intentionally does NOT contain:

        - network fetches
        - database writes
        - inbox delivery logic
        - one-off live instance repair rules
*/

use serde_json::{json, Value};

/* ------------------------------------------------------------------------- */
/* Target families                                                           */
/* ------------------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupTargetFamily {
    ThreadiverseForum,
    CollectionChannel,
    RelayBot,
    BlogPublisher,
    ProfileOnly,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupTarget {
    Lotide,
    Lemmy,
    PieFed,
    Kbin,
    Mbin,
    NodeBb,
    Discourse,
    Friendica,
    Mobilizon,
    PeerTube,
    Smithereen,
    Hubzilla,
    StreamsForte,
    Bonfire,
    Flipboard,
    Elgg,
    Gancio,
    Guppe,
    Fedigroup,
    FediGroups,
    FedibirdGroup,
    ApGroups,
    GroupActor,
    TootGroup,
    BuzzRelay,
    Funkwhale,
    WordPress,
    WordPressEventBridge,
    Mastodon,
    Pleroma,
    UnknownGroup,
    UnknownActor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetActorKind {
    Group,
    Person,
    Service,
    Application,
    Unknown,
}

impl GroupTarget {
    pub fn family(self) -> GroupTargetFamily {
        target_spec(self).family
    }

    pub fn as_str(self) -> &'static str {
        match self {
            GroupTarget::Lotide => "Lotide",
            GroupTarget::Lemmy => "Lemmy",
            GroupTarget::PieFed => "PieFed",
            GroupTarget::Kbin => "Kbin",
            GroupTarget::Mbin => "Mbin",
            GroupTarget::NodeBb => "NodeBb",
            GroupTarget::Discourse => "Discourse",
            GroupTarget::Friendica => "Friendica",
            GroupTarget::Mobilizon => "Mobilizon",
            GroupTarget::PeerTube => "PeerTube",
            GroupTarget::Smithereen => "Smithereen",
            GroupTarget::Hubzilla => "Hubzilla",
            GroupTarget::StreamsForte => "StreamsForte",
            GroupTarget::Bonfire => "Bonfire",
            GroupTarget::Flipboard => "Flipboard",
            GroupTarget::Elgg => "Elgg",
            GroupTarget::Gancio => "Gancio",
            GroupTarget::Guppe => "Guppe",
            GroupTarget::Fedigroup => "Fedigroup",
            GroupTarget::FediGroups => "FediGroups",
            GroupTarget::FedibirdGroup => "FedibirdGroup",
            GroupTarget::ApGroups => "ApGroups",
            GroupTarget::GroupActor => "GroupActor",
            GroupTarget::TootGroup => "TootGroup",
            GroupTarget::BuzzRelay => "BuzzRelay",
            GroupTarget::Funkwhale => "Funkwhale",
            GroupTarget::WordPress => "WordPress",
            GroupTarget::WordPressEventBridge => "WordPressEventBridge",
            GroupTarget::Mastodon => "Mastodon",
            GroupTarget::Pleroma => "Pleroma",
            GroupTarget::UnknownGroup => "UnknownGroup",
            GroupTarget::UnknownActor => "UnknownActor",
        }
    }
}

impl GroupTargetFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            GroupTargetFamily::ThreadiverseForum => "ThreadiverseForum",
            GroupTargetFamily::CollectionChannel => "CollectionChannel",
            GroupTargetFamily::RelayBot => "RelayBot",
            GroupTargetFamily::BlogPublisher => "BlogPublisher",
            GroupTargetFamily::ProfileOnly => "ProfileOnly",
            GroupTargetFamily::Unknown => "Unknown",
        }
    }
}

impl TargetActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TargetActorKind::Group => "Group",
            TargetActorKind::Person => "Person",
            TargetActorKind::Service => "Service",
            TargetActorKind::Application => "Application",
            TargetActorKind::Unknown => "Unknown",
        }
    }
}

/* ------------------------------------------------------------------------- */
/* Operation matrix                                                          */
/* ------------------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FederationOperation {
    Follow,
    Unfollow,
    CreatePost,
    DeletePost,
    Comment,
    DeleteComment,
    Like,
    UndoLike,
    ReceiveFollow,
    ReceiveUnfollow,
    ReceivePost,
    ReceiveDeletePost,
    ReceiveComment,
    ReceiveDeleteComment,
    ReceiveLike,
    ReceiveUndoLike,
    Dislike,
    UndoDislike,
    Moderate,
    PreviewHistory,
}

pub const FEDERATION_OPERATIONS: &[FederationOperation] = &[
    FederationOperation::Follow,
    FederationOperation::Unfollow,
    FederationOperation::CreatePost,
    FederationOperation::DeletePost,
    FederationOperation::Comment,
    FederationOperation::DeleteComment,
    FederationOperation::Like,
    FederationOperation::UndoLike,
    FederationOperation::ReceiveFollow,
    FederationOperation::ReceiveUnfollow,
    FederationOperation::ReceivePost,
    FederationOperation::ReceiveDeletePost,
    FederationOperation::ReceiveComment,
    FederationOperation::ReceiveDeleteComment,
    FederationOperation::ReceiveLike,
    FederationOperation::ReceiveUndoLike,
    FederationOperation::Dislike,
    FederationOperation::UndoDislike,
    FederationOperation::Moderate,
    FederationOperation::PreviewHistory,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationSupport {
    Required,
    BestEffort,
    InboundOnly,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetCapabilities {
    follow: OperationSupport,
    unfollow: OperationSupport,
    create_post: OperationSupport,
    delete_post: OperationSupport,
    comment: OperationSupport,
    delete_comment: OperationSupport,
    like: OperationSupport,
    undo_like: OperationSupport,
    receive_follow: OperationSupport,
    receive_unfollow: OperationSupport,
    receive_post: OperationSupport,
    receive_delete_post: OperationSupport,
    receive_comment: OperationSupport,
    receive_delete_comment: OperationSupport,
    receive_like: OperationSupport,
    receive_undo_like: OperationSupport,
    dislike: OperationSupport,
    undo_dislike: OperationSupport,
    moderate: OperationSupport,
    preview_history: OperationSupport,
}

impl TargetCapabilities {
    const fn none() -> Self {
        Self {
            follow: OperationSupport::Unsupported,
            unfollow: OperationSupport::Unsupported,
            create_post: OperationSupport::Unsupported,
            delete_post: OperationSupport::Unsupported,
            comment: OperationSupport::Unsupported,
            delete_comment: OperationSupport::Unsupported,
            like: OperationSupport::Unsupported,
            undo_like: OperationSupport::Unsupported,
            receive_follow: OperationSupport::Unsupported,
            receive_unfollow: OperationSupport::Unsupported,
            receive_post: OperationSupport::Unsupported,
            receive_delete_post: OperationSupport::Unsupported,
            receive_comment: OperationSupport::Unsupported,
            receive_delete_comment: OperationSupport::Unsupported,
            receive_like: OperationSupport::Unsupported,
            receive_undo_like: OperationSupport::Unsupported,
            dislike: OperationSupport::Unsupported,
            undo_dislike: OperationSupport::Unsupported,
            moderate: OperationSupport::Unsupported,
            preview_history: OperationSupport::Unsupported,
        }
    }

    const fn threadiverse() -> Self {
        Self {
            follow: OperationSupport::Required,
            unfollow: OperationSupport::Required,
            create_post: OperationSupport::Required,
            delete_post: OperationSupport::Required,
            comment: OperationSupport::Required,
            delete_comment: OperationSupport::Required,
            like: OperationSupport::Required,
            undo_like: OperationSupport::Required,
            receive_follow: OperationSupport::Required,
            receive_unfollow: OperationSupport::Required,
            receive_post: OperationSupport::Required,
            receive_delete_post: OperationSupport::Required,
            receive_comment: OperationSupport::Required,
            receive_delete_comment: OperationSupport::Required,
            receive_like: OperationSupport::Required,
            receive_undo_like: OperationSupport::Required,
            dislike: OperationSupport::InboundOnly,
            undo_dislike: OperationSupport::InboundOnly,
            moderate: OperationSupport::BestEffort,
            preview_history: OperationSupport::BestEffort,
        }
    }

    const fn collection_channel() -> Self {
        Self {
            follow: OperationSupport::Required,
            unfollow: OperationSupport::Required,
            create_post: OperationSupport::BestEffort,
            delete_post: OperationSupport::BestEffort,
            comment: OperationSupport::BestEffort,
            delete_comment: OperationSupport::BestEffort,
            like: OperationSupport::BestEffort,
            undo_like: OperationSupport::BestEffort,
            receive_follow: OperationSupport::BestEffort,
            receive_unfollow: OperationSupport::BestEffort,
            receive_post: OperationSupport::Required,
            receive_delete_post: OperationSupport::BestEffort,
            receive_comment: OperationSupport::BestEffort,
            receive_delete_comment: OperationSupport::BestEffort,
            receive_like: OperationSupport::BestEffort,
            receive_undo_like: OperationSupport::BestEffort,
            dislike: OperationSupport::InboundOnly,
            undo_dislike: OperationSupport::InboundOnly,
            moderate: OperationSupport::BestEffort,
            preview_history: OperationSupport::BestEffort,
        }
    }

    const fn relay_bot() -> Self {
        Self {
            follow: OperationSupport::Required,
            unfollow: OperationSupport::Required,
            create_post: OperationSupport::BestEffort,
            delete_post: OperationSupport::BestEffort,
            comment: OperationSupport::Unsupported,
            delete_comment: OperationSupport::Unsupported,
            like: OperationSupport::Unsupported,
            undo_like: OperationSupport::Unsupported,
            receive_follow: OperationSupport::BestEffort,
            receive_unfollow: OperationSupport::BestEffort,
            receive_post: OperationSupport::Required,
            receive_delete_post: OperationSupport::BestEffort,
            receive_comment: OperationSupport::BestEffort,
            receive_delete_comment: OperationSupport::BestEffort,
            receive_like: OperationSupport::Unsupported,
            receive_undo_like: OperationSupport::Unsupported,
            dislike: OperationSupport::Unsupported,
            undo_dislike: OperationSupport::Unsupported,
            moderate: OperationSupport::Unsupported,
            preview_history: OperationSupport::Unsupported,
        }
    }

    const fn blog_publisher() -> Self {
        Self {
            follow: OperationSupport::BestEffort,
            unfollow: OperationSupport::BestEffort,
            create_post: OperationSupport::Unsupported,
            delete_post: OperationSupport::Unsupported,
            comment: OperationSupport::BestEffort,
            delete_comment: OperationSupport::BestEffort,
            like: OperationSupport::BestEffort,
            undo_like: OperationSupport::BestEffort,
            receive_follow: OperationSupport::BestEffort,
            receive_unfollow: OperationSupport::BestEffort,
            receive_post: OperationSupport::Required,
            receive_delete_post: OperationSupport::BestEffort,
            receive_comment: OperationSupport::BestEffort,
            receive_delete_comment: OperationSupport::BestEffort,
            receive_like: OperationSupport::BestEffort,
            receive_undo_like: OperationSupport::BestEffort,
            dislike: OperationSupport::Unsupported,
            undo_dislike: OperationSupport::Unsupported,
            moderate: OperationSupport::Unsupported,
            preview_history: OperationSupport::BestEffort,
        }
    }

    const fn profile_only() -> Self {
        Self {
            follow: OperationSupport::Required,
            unfollow: OperationSupport::Required,
            receive_follow: OperationSupport::Required,
            receive_unfollow: OperationSupport::Required,
            receive_post: OperationSupport::BestEffort,
            receive_delete_post: OperationSupport::BestEffort,
            receive_comment: OperationSupport::BestEffort,
            receive_delete_comment: OperationSupport::BestEffort,
            receive_like: OperationSupport::BestEffort,
            receive_undo_like: OperationSupport::BestEffort,
            ..Self::none()
        }
    }

    const fn for_family(family: GroupTargetFamily) -> Self {
        match family {
            GroupTargetFamily::ThreadiverseForum => Self::threadiverse(),
            GroupTargetFamily::CollectionChannel => Self::collection_channel(),
            GroupTargetFamily::RelayBot => Self::relay_bot(),
            GroupTargetFamily::BlogPublisher => Self::blog_publisher(),
            GroupTargetFamily::ProfileOnly => Self::profile_only(),
            GroupTargetFamily::Unknown => Self::none(),
        }
    }

    pub fn support(self, operation: FederationOperation) -> OperationSupport {
        match operation {
            FederationOperation::Follow => self.follow,
            FederationOperation::Unfollow => self.unfollow,
            FederationOperation::CreatePost => self.create_post,
            FederationOperation::DeletePost => self.delete_post,
            FederationOperation::Comment => self.comment,
            FederationOperation::DeleteComment => self.delete_comment,
            FederationOperation::Like => self.like,
            FederationOperation::UndoLike => self.undo_like,
            FederationOperation::ReceiveFollow => self.receive_follow,
            FederationOperation::ReceiveUnfollow => self.receive_unfollow,
            FederationOperation::ReceivePost => self.receive_post,
            FederationOperation::ReceiveDeletePost => self.receive_delete_post,
            FederationOperation::ReceiveComment => self.receive_comment,
            FederationOperation::ReceiveDeleteComment => self.receive_delete_comment,
            FederationOperation::ReceiveLike => self.receive_like,
            FederationOperation::ReceiveUndoLike => self.receive_undo_like,
            FederationOperation::Dislike => self.dislike,
            FederationOperation::UndoDislike => self.undo_dislike,
            FederationOperation::Moderate => self.moderate,
            FederationOperation::PreviewHistory => self.preview_history,
        }
    }
}

/* ------------------------------------------------------------------------- */
/* Static target registry                                                    */
/* ------------------------------------------------------------------------- */

pub struct GroupTargetSpec {
    pub target: GroupTarget,
    pub family: GroupTargetFamily,
    pub software_names: &'static [&'static str],
    pub actor_path_hints: &'static [&'static str],
    pub object_types: &'static [&'static str],
    pub activity_types: &'static [&'static str],
    pub capabilities: TargetCapabilities,
}

const THREADIVERSE_OBJECTS: &[&str] = &["Page", "Article", "Note", "Question"];
const THREADIVERSE_ACTIVITIES: &[&str] = &[
    "Accept", "Announce", "Create", "Update", "Delete", "Remove", "Like", "Dislike", "Undo",
    "Follow",
];
const CHANNEL_OBJECTS: &[&str] = &[
    "Article", "Audio", "Document", "Event", "Image", "Note", "Page", "Video",
];
const CHANNEL_ACTIVITIES: &[&str] = &[
    "Accept", "Announce", "Create", "Update", "Delete", "Add", "Remove", "Join", "Leave", "Like",
    "Undo", "Follow",
];
const RELAY_OBJECTS: &[&str] = &["Note", "Article", "Page"];
const RELAY_ACTIVITIES: &[&str] = &["Accept", "Announce", "Create", "Delete", "Follow", "Undo"];
const BLOG_OBJECTS: &[&str] = &["Article", "Note", "Page"];
const BLOG_ACTIVITIES: &[&str] = &["Accept", "Create", "Update", "Delete", "Follow", "Undo"];
const PROFILE_OBJECTS: &[&str] = &["Note", "Article", "Page"];
const PROFILE_ACTIVITIES: &[&str] = &["Accept", "Create", "Delete", "Follow", "Like", "Undo"];

pub const COMMON_ACTOR_PATH_PREFIXES: &[&[&str]] = &[
    &["c"],
    &["m"],
    &["video-channels"],
    &["channels"],
    &["events"],
    &["event"],
    &["profile"],
    &["channel"],
    &["category"],
    &["categories"],
    &["groups"],
    &["group"],
    &["activitypub", "group"],
    &["activitypub", "groups"],
    &["federation", "u"],
    &["author"],
    &["authors"],
    &["u"],
    &["users"],
];

pub const TARGET_SPECS: &[GroupTargetSpec] = &[
    GroupTargetSpec {
        target: GroupTarget::Lotide,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["lotide"],
        actor_path_hints: &["/apub/communities/", "/communities/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Lemmy,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["lemmy"],
        actor_path_hints: &["/c/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::PieFed,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["piefed"],
        actor_path_hints: &["/c/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Kbin,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["kbin"],
        actor_path_hints: &["/m/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Mbin,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["mbin", "fedia"],
        actor_path_hints: &["/m/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::NodeBb,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["nodebb"],
        actor_path_hints: &["/category/", "/categories/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Discourse,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["discourse"],
        actor_path_hints: &["/ap/category/", "/ap/tag/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Friendica,
        family: GroupTargetFamily::ThreadiverseForum,
        software_names: &["friendica"],
        actor_path_hints: &["/profile/"],
        object_types: THREADIVERSE_OBJECTS,
        activity_types: THREADIVERSE_ACTIVITIES,
        capabilities: TargetCapabilities::threadiverse(),
    },
    GroupTargetSpec {
        target: GroupTarget::Mobilizon,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["mobilizon"],
        actor_path_hints: &["/groups/", "/@"],
        object_types: &["Article", "Event", "Note"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::PeerTube,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["peertube"],
        actor_path_hints: &["/video-channels/", "/c/"],
        object_types: &["Video", "Note"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Smithereen,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["smithereen"],
        actor_path_hints: &["/groups/"],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Hubzilla,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["hubzilla"],
        actor_path_hints: &["/channel/"],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::StreamsForte,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["streams", "forte"],
        actor_path_hints: &["/channel/"],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Bonfire,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["bonfire"],
        actor_path_hints: &["/pub/actors/", "/@", "/groups/"],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Flipboard,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["flipboard"],
        actor_path_hints: &["/@"],
        object_types: &["Article", "Note", "Page"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Elgg,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["elgg"],
        actor_path_hints: &["/activitypub/group/", "/activitypub/groups/"],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Gancio,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["gancio"],
        actor_path_hints: &["/federation/u/", "/events/"],
        object_types: &["Event", "Note"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Guppe,
        family: GroupTargetFamily::RelayBot,
        software_names: &["guppe"],
        actor_path_hints: &["/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::FediGroups,
        family: GroupTargetFamily::RelayBot,
        software_names: &["fedigroups", "fedigroups.social"],
        actor_path_hints: &["/@", "/groups/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::Fedigroup,
        family: GroupTargetFamily::RelayBot,
        software_names: &["fedigroup"],
        actor_path_hints: &["/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::FedibirdGroup,
        family: GroupTargetFamily::RelayBot,
        software_names: &["fedibird", "fedibird group"],
        actor_path_hints: &["/@", "/users/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::ApGroups,
        family: GroupTargetFamily::RelayBot,
        software_names: &["ap-groups", "chirp.social"],
        actor_path_hints: &["/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::GroupActor,
        family: GroupTargetFamily::RelayBot,
        software_names: &["group actor", "group-actor"],
        actor_path_hints: &["/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::TootGroup,
        family: GroupTargetFamily::RelayBot,
        software_names: &["tootgroup", "mastodon group bot"],
        actor_path_hints: &["/@", "/users/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::BuzzRelay,
        family: GroupTargetFamily::RelayBot,
        software_names: &["buzzrelay"],
        actor_path_hints: &["/tag/", "/instance/"],
        object_types: RELAY_OBJECTS,
        activity_types: RELAY_ACTIVITIES,
        capabilities: TargetCapabilities::relay_bot(),
    },
    GroupTargetSpec {
        target: GroupTarget::Funkwhale,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["funkwhale"],
        actor_path_hints: &["/channels/", "/federation/music/libraries/"],
        object_types: &["Audio", "AudioCollection", "Library", "Document", "Note"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::WordPress,
        family: GroupTargetFamily::BlogPublisher,
        software_names: &["wordpress", "activitypub plugin for wordpress"],
        actor_path_hints: &["/author/", "/authors/", "/@"],
        object_types: BLOG_OBJECTS,
        activity_types: BLOG_ACTIVITIES,
        capabilities: TargetCapabilities::blog_publisher(),
    },
    GroupTargetSpec {
        target: GroupTarget::WordPressEventBridge,
        family: GroupTargetFamily::CollectionChannel,
        software_names: &["event bridge for activitypub", "wordpress event bridge"],
        actor_path_hints: &["/events/", "/event/"],
        object_types: &["Event", "Note"],
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::Mastodon,
        family: GroupTargetFamily::ProfileOnly,
        software_names: &["mastodon"],
        actor_path_hints: &["/@", "/users/"],
        object_types: PROFILE_OBJECTS,
        activity_types: PROFILE_ACTIVITIES,
        capabilities: TargetCapabilities::profile_only(),
    },
    GroupTargetSpec {
        target: GroupTarget::Pleroma,
        family: GroupTargetFamily::ProfileOnly,
        software_names: &["pleroma", "akkoma"],
        actor_path_hints: &["/users/"],
        object_types: PROFILE_OBJECTS,
        activity_types: PROFILE_ACTIVITIES,
        capabilities: TargetCapabilities::profile_only(),
    },
    GroupTargetSpec {
        target: GroupTarget::UnknownGroup,
        family: GroupTargetFamily::Unknown,
        software_names: &[],
        actor_path_hints: &[],
        object_types: CHANNEL_OBJECTS,
        activity_types: CHANNEL_ACTIVITIES,
        capabilities: TargetCapabilities::collection_channel(),
    },
    GroupTargetSpec {
        target: GroupTarget::UnknownActor,
        family: GroupTargetFamily::Unknown,
        software_names: &[],
        actor_path_hints: &[],
        object_types: &[],
        activity_types: &[],
        capabilities: TargetCapabilities::none(),
    },
];

pub fn target_spec(target: GroupTarget) -> &'static GroupTargetSpec {
    TARGET_SPECS
        .iter()
        .find(|spec| spec.target == target)
        .unwrap_or_else(|| {
            TARGET_SPECS
                .iter()
                .find(|spec| spec.target == GroupTarget::UnknownActor)
                .expect("UnknownActor target spec must exist")
        })
}

/* ------------------------------------------------------------------------- */
/* Actor classification                                                      */
/* ------------------------------------------------------------------------- */

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetProfile {
    pub target: GroupTarget,
    pub family: GroupTargetFamily,
    pub actor_kind: TargetActorKind,
    pub has_inbox: bool,
    pub has_outbox: bool,
    pub has_followers: bool,
    pub has_featured: bool,
}

impl TargetProfile {
    pub fn support(&self, operation: FederationOperation) -> OperationSupport {
        if !self.has_inbox && needs_target_inbox(operation) {
            return OperationSupport::Unsupported;
        }

        self.capabilities().support(operation)
    }

    pub fn source(&self) -> &'static str {
        if self.is_registry_target() {
            "registry"
        } else if self.family != GroupTargetFamily::Unknown {
            "heuristic"
        } else {
            "unknown"
        }
    }

    pub fn confidence(&self) -> i16 {
        if self.is_registry_target() {
            90
        } else if self.family != GroupTargetFamily::Unknown {
            40
        } else {
            0
        }
    }

    pub fn evidence_json(&self) -> Value {
        json!({
            "source": self.source(),
            "actor": {
                "has_inbox": self.has_inbox,
                "has_outbox": self.has_outbox,
                "has_followers": self.has_followers,
                "has_featured": self.has_featured
            }
        })
    }

    fn is_registry_target(&self) -> bool {
        !matches!(
            self.target,
            GroupTarget::UnknownGroup | GroupTarget::UnknownActor
        )
    }

    fn capabilities(&self) -> TargetCapabilities {
        if self.is_registry_target() {
            target_spec(self.target).capabilities
        } else {
            TargetCapabilities::for_family(self.family)
        }
    }
}

fn needs_target_inbox(operation: FederationOperation) -> bool {
    matches!(
        operation,
        FederationOperation::Follow
            | FederationOperation::Unfollow
            | FederationOperation::CreatePost
            | FederationOperation::DeletePost
            | FederationOperation::Comment
            | FederationOperation::DeleteComment
            | FederationOperation::Like
            | FederationOperation::UndoLike
            | FederationOperation::Dislike
            | FederationOperation::UndoDislike
            | FederationOperation::Moderate
    )
}

pub fn classify_actor_value(value: &Value) -> TargetProfile {
    let actor_kind = actor_kind(value);
    let target = classify_target(value, actor_kind);
    let has_inbox = value.get("inbox").and_then(Value::as_str).is_some();
    let has_outbox = value.get("outbox").and_then(Value::as_str).is_some();
    let has_followers = value.get("followers").and_then(Value::as_str).is_some();
    let has_featured = value.get("featured").is_some();
    let family = if target.family() == GroupTargetFamily::Unknown {
        infer_unknown_family(actor_kind, has_outbox, has_followers, has_featured)
    } else {
        target.family()
    };

    TargetProfile {
        target,
        family,
        actor_kind,
        has_inbox,
        has_outbox,
        has_followers,
        has_featured,
    }
}

pub fn known_object_type(object: &super::KnownObject) -> Option<&'static str> {
    match object {
        super::KnownObject::Article(_) => Some("Article"),
        super::KnownObject::Audio(_) => Some("Audio"),
        super::KnownObject::Document(_) => Some("Document"),
        super::KnownObject::Event(_) => Some("Event"),
        super::KnownObject::Image(_) => Some("Image"),
        super::KnownObject::FunkwhaleLibrary(_) => Some("Library"),
        super::KnownObject::Note(_) => Some("Note"),
        super::KnownObject::Page(_) => Some("Page"),
        super::KnownObject::Question(_) => Some("Question"),
        super::KnownObject::Video(_) => Some("Video"),
        _ => None,
    }
}

pub fn classify_known_object(object: &super::KnownObject) -> Option<TargetProfile> {
    match object {
        super::KnownObject::Group(_)
        | super::KnownObject::Person(_)
        | super::KnownObject::Application(_)
        | super::KnownObject::Service(_) => {
            let value = serde_json::to_value(object).ok()?;
            Some(classify_actor_value(&value))
        }
        super::KnownObject::FunkwhaleLibrary(_) => {
            let value = serde_json::to_value(object).ok()?;
            Some(TargetProfile {
                target: GroupTarget::Funkwhale,
                family: GroupTargetFamily::CollectionChannel,
                actor_kind: TargetActorKind::Unknown,
                has_inbox: false,
                has_outbox: value.get("outbox").and_then(Value::as_str).is_some(),
                has_followers: value.get("followers").and_then(Value::as_str).is_some(),
                has_featured: false,
            })
        }
        _ => None,
    }
}

fn actor_kind(value: &Value) -> TargetActorKind {
    match value.get("type").and_then(Value::as_str) {
        Some("Group") => TargetActorKind::Group,
        Some("Person") => TargetActorKind::Person,
        Some("Service") => TargetActorKind::Service,
        Some("Application") => TargetActorKind::Application,
        _ => TargetActorKind::Unknown,
    }
}

fn infer_unknown_family(
    actor_kind: TargetActorKind,
    has_outbox: bool,
    _has_followers: bool,
    has_featured: bool,
) -> GroupTargetFamily {
    /*
        Unknown actor fallback

        Some working group software is only partly self-describing. If the
        actor has the basic shape of a channel or relay, lotide keeps a
        conservative target family instead of rejecting it outright. Later
        object observations can refine this profile without rerunning the
        whole heuristic chain for every lookup.
    */
    match actor_kind {
        TargetActorKind::Group => GroupTargetFamily::CollectionChannel,
        TargetActorKind::Service | TargetActorKind::Application => {
            if has_outbox || has_featured {
                GroupTargetFamily::CollectionChannel
            } else {
                GroupTargetFamily::RelayBot
            }
        }
        TargetActorKind::Person => GroupTargetFamily::ProfileOnly,
        TargetActorKind::Unknown => GroupTargetFamily::Unknown,
    }
}

fn classify_target(value: &Value, actor_kind: TargetActorKind) -> GroupTarget {
    let metadata_target = target_from_classification_metadata(value);

    if let Some(id) = value.get("id").and_then(Value::as_str) {
        if let Ok(url) = id.parse::<url::Url>() {
            let host = url.host_str().unwrap_or("");
            let path = url.path();

            /*
                Actor identity beats software text.

                Some group actors include Mastodon-related text in their
                descriptions or context metadata, while their stable AP ID is
                plainly a threadiverse `/c/` or `/m/` actor. Path and host
                hints therefore run before broad software-name scans.
            */
            if contains_ci(host, "piefed") {
                return GroupTarget::PieFed;
            }
            if contains_ci(host, "kbin.melroy.org") {
                return GroupTarget::Mbin;
            }
            if contains_ci(host, "mbin") || contains_ci(host, "fedia") {
                return GroupTarget::Mbin;
            }
            if contains_ci(host, "flipboard.com") {
                return GroupTarget::Flipboard;
            }
            if contains_ci(host, "fedigroups.social") {
                return GroupTarget::FediGroups;
            }
            if contains_ci(host, "gdev.fedibird.com") {
                return GroupTarget::FedibirdGroup;
            }
            if contains_ci(host, "relay.fedi.buzz") {
                return GroupTarget::BuzzRelay;
            }
            if actor_has_any(value, &["wall"]) {
                /*
                    Smithereen-style groups expose a wall collection as the
                    appendable group surface. That is a stronger signal than
                    path spelling because live Smithereen instances and later
                    compatible group work may choose different URL layouts.
                */
                return GroupTarget::Smithereen;
            }
            if contains_ci(host, "mobilizon")
                || actor_has_any(value, &["events", "members", "resources"])
            {
                return GroupTarget::Mobilizon;
            }
            if contains_ci(host, "discourse") || path.starts_with("/ap/actor/") {
                return GroupTarget::Discourse;
            }
            if actor_has_wp_activitypub_path(value)
                || url.query().is_some_and(|query| query.contains("author="))
            {
                return GroupTarget::WordPress;
            }
            if contains_ci(host, "peertube") || path.starts_with("/video-channels/") {
                return GroupTarget::PeerTube;
            }
            if contains_ci(host, "nodebb") || path.starts_with("/category/") {
                return GroupTarget::NodeBb;
            }
            if let Some(target) = target_from_actor_path(path) {
                return target;
            }
            if path.starts_with("/apub/communities/") || path.starts_with("/communities/") {
                return GroupTarget::Lotide;
            }
            if path.starts_with("/m/") {
                /*
                    Mbin kept Kbin's magazine URL layout, and several active
                    Mbin instances still have kbin in their host name. Host
                    spelling alone is therefore not enough evidence for Kbin.
                    A true Kbin actor can still identify itself through
                    generator or software metadata before this fallback.
                */
                if let Some(target @ (GroupTarget::Kbin | GroupTarget::Mbin)) = metadata_target {
                    return target;
                }

                return GroupTarget::Mbin;
            }
            if path.starts_with("/c/") {
                return GroupTarget::Lemmy;
            }
            if path.starts_with("/profile/") {
                return GroupTarget::Friendica;
            }
            if path.starts_with("/channel/") {
                return GroupTarget::Hubzilla;
            }
            if path.starts_with("/channels/") {
                return GroupTarget::Funkwhale;
            }
            if path.starts_with("/author/") || path.starts_with("/authors/") {
                return GroupTarget::WordPress;
            }
        }
    }

    if let Some(target) = metadata_target {
        return target;
    }

    match actor_kind {
        TargetActorKind::Group => GroupTarget::UnknownGroup,
        _ => GroupTarget::UnknownActor,
    }
}

fn target_from_classification_metadata(value: &Value) -> Option<GroupTarget> {
    let haystack = classification_text(value);

    for spec in TARGET_SPECS {
        if spec.target == GroupTarget::UnknownActor || spec.target == GroupTarget::UnknownGroup {
            continue;
        }

        for software_name in spec.software_names {
            if contains_ci(&haystack, software_name) {
                return Some(spec.target);
            }
        }
    }

    None
}

fn target_from_actor_path(path: &str) -> Option<GroupTarget> {
    for spec in TARGET_SPECS {
        if spec.target == GroupTarget::UnknownActor || spec.target == GroupTarget::UnknownGroup {
            continue;
        }

        for hint in spec.actor_path_hints {
            if matches!(
                *hint,
                "/" | "/@" | "/c/" | "/m/" | "/u/" | "/users/" | "/group/" | "/groups/"
            ) {
                continue;
            }

            if path.starts_with(hint) {
                return Some(spec.target);
            }
        }
    }

    None
}

fn classification_text(value: &Value) -> String {
    let mut out = String::new();
    collect_classification_text(value, &mut out);
    out
}

fn collect_classification_text(value: &Value, out: &mut String) {
    if let Value::Object(values) = value {
        /*
            Classification metadata

            Human-written names and summaries frequently mention other
            fediverse software. They are useful display text, but they must not
            override actor IDs, path hints, or generator/software metadata.
        */
        for key in ["type", "generator", "software"] {
            if let Some(value) = values.get(key) {
                collect_classification_metadata_text(value, out);
            }
        }
    }
}

fn collect_classification_metadata_text(value: &Value, out: &mut String) {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
        Value::String(value) => {
            out.push(' ');
            out.push_str(value);
        }
        Value::Array(values) => {
            for value in values {
                collect_classification_metadata_text(value, out);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_classification_metadata_text(value, out);
            }
        }
    }
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn actor_has_any(value: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|key| value.get(*key).is_some())
}

fn actor_has_wp_activitypub_path(value: &Value) -> bool {
    ["id", "inbox", "outbox", "followers"].iter().any(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|url| contains_ci(url, "/wp-json/activitypub/"))
    })
}

/* ------------------------------------------------------------------------- */
/* Tests                                                                     */
/* ------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use super::{
        classify_actor_value, classify_known_object, target_spec, FederationOperation, GroupTarget,
        GroupTargetFamily, OperationSupport, TargetActorKind, FEDERATION_OPERATIONS, TARGET_SPECS,
    };

    const REPORT_TARGETS: &[GroupTarget] = &[
        GroupTarget::Lotide,
        GroupTarget::Lemmy,
        GroupTarget::Mbin,
        GroupTarget::PieFed,
        GroupTarget::NodeBb,
        GroupTarget::Discourse,
        GroupTarget::Friendica,
        GroupTarget::Kbin,
        GroupTarget::Mobilizon,
        GroupTarget::PeerTube,
        GroupTarget::Smithereen,
        GroupTarget::Hubzilla,
        GroupTarget::StreamsForte,
        GroupTarget::Bonfire,
        GroupTarget::Flipboard,
        GroupTarget::Elgg,
        GroupTarget::Gancio,
        GroupTarget::Guppe,
        GroupTarget::Fedigroup,
        GroupTarget::FediGroups,
        GroupTarget::FedibirdGroup,
        GroupTarget::ApGroups,
        GroupTarget::GroupActor,
        GroupTarget::TootGroup,
        GroupTarget::BuzzRelay,
        GroupTarget::Funkwhale,
        GroupTarget::WordPress,
        GroupTarget::WordPressEventBridge,
        GroupTarget::Mastodon,
        GroupTarget::Pleroma,
    ];

    #[test]
    fn report_targets_have_registry_entries() {
        assert_eq!(FEDERATION_OPERATIONS.len(), 20);

        for target in REPORT_TARGETS {
            let spec = target_spec(*target);

            assert_eq!(spec.target, *target);
            assert_ne!(spec.family, GroupTargetFamily::Unknown, "{target:?}");
            assert!(!spec.object_types.is_empty(), "{target:?}");
            assert!(!spec.activity_types.is_empty(), "{target:?}");
        }
    }

    #[test]
    fn registry_does_not_accidentally_duplicate_targets() {
        for (idx, spec) in TARGET_SPECS.iter().enumerate() {
            assert!(
                TARGET_SPECS[(idx + 1)..]
                    .iter()
                    .all(|other| other.target != spec.target),
                "duplicate target {:?}",
                spec.target
            );
        }
    }

    #[test]
    fn threadiverse_targets_require_core_link_aggregator_actions() {
        for target in [
            GroupTarget::Lotide,
            GroupTarget::Lemmy,
            GroupTarget::Mbin,
            GroupTarget::PieFed,
            GroupTarget::NodeBb,
            GroupTarget::Discourse,
            GroupTarget::Friendica,
            GroupTarget::Kbin,
        ] {
            let capabilities = target_spec(target).capabilities;

            assert_eq!(
                capabilities.support(FederationOperation::Follow),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::CreatePost),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::Comment),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::Like),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::Dislike),
                OperationSupport::InboundOnly,
                "{target:?}"
            );
        }
    }

    #[test]
    fn channel_targets_do_not_claim_first_class_threadiverse_posting() {
        for target in [
            GroupTarget::Mobilizon,
            GroupTarget::PeerTube,
            GroupTarget::Smithereen,
            GroupTarget::Hubzilla,
            GroupTarget::StreamsForte,
            GroupTarget::Bonfire,
            GroupTarget::Flipboard,
            GroupTarget::Elgg,
            GroupTarget::Gancio,
            GroupTarget::Funkwhale,
            GroupTarget::WordPressEventBridge,
        ] {
            let capabilities = target_spec(target).capabilities;

            assert_eq!(
                capabilities.support(FederationOperation::ReceivePost),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_ne!(
                capabilities.support(FederationOperation::CreatePost),
                OperationSupport::Required,
                "{target:?}"
            );
        }
    }

    #[test]
    fn relay_targets_are_follow_and_announce_oriented() {
        for target in [
            GroupTarget::Guppe,
            GroupTarget::Fedigroup,
            GroupTarget::FediGroups,
            GroupTarget::FedibirdGroup,
            GroupTarget::ApGroups,
            GroupTarget::GroupActor,
            GroupTarget::TootGroup,
        ] {
            let capabilities = target_spec(target).capabilities;

            assert_eq!(
                capabilities.support(FederationOperation::Follow),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::ReceivePost),
                OperationSupport::Required,
                "{target:?}"
            );
            assert_eq!(
                capabilities.support(FederationOperation::Like),
                OperationSupport::Unsupported,
                "{target:?}"
            );
        }
    }

    #[test]
    fn actor_classifier_identifies_known_platform_shapes() {
        let cases = [
            (
                GroupTarget::Lemmy,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://hilariouschaos.com/c/positivity",
                    "inbox": "https://hilariouschaos.com/c/positivity/inbox",
                    "outbox": "https://hilariouschaos.com/c/positivity/outbox",
                    "followers": "https://hilariouschaos.com/c/positivity/followers"
                }),
            ),
            (
                GroupTarget::Lemmy,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://diggita.com/c/opensource",
                    "inbox": "https://diggita.com/c/opensource/inbox",
                    "outbox": "https://diggita.com/c/opensource/outbox",
                    "followers": "https://diggita.com/c/opensource/followers",
                    "generator": {"name": "Mastodon-compatible renderer"},
                    "summary": "A group for open source people from mastodon.uno."
                }),
            ),
            (
                GroupTarget::PieFed,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://piefed.social/c/historymemes",
                    "inbox": "https://piefed.social/c/historymemes/inbox",
                    "outbox": "https://piefed.social/c/historymemes/outbox"
                }),
            ),
            (
                GroupTarget::Mbin,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://kbin.earth/m/random",
                    "@context": ["https://www.w3.org/ns/activitystreams", "https://kbin.earth/contexts"],
                    "inbox": "https://kbin.earth/m/random/inbox"
                }),
            ),
            (
                GroupTarget::Kbin,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://kbin.example/m/random",
                    "generator": {"name": "Kbin"},
                    "inbox": "https://kbin.example/m/random/inbox"
                }),
            ),
            (
                GroupTarget::Mbin,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://fedia.io/m/privacy",
                    "inbox": "https://fedia.io/m/privacy/inbox"
                }),
            ),
            (
                GroupTarget::Mbin,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://kbin.melroy.org/m/updates",
                    "generator": {"name": "Mbin"},
                    "inbox": "https://kbin.melroy.org/m/updates/inbox"
                }),
            ),
            (
                GroupTarget::PeerTube,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://spectra.video/video-channels/fediforum_demos",
                    "inbox": "https://spectra.video/video-channels/fediforum_demos/inbox"
                }),
            ),
            (
                GroupTarget::BuzzRelay,
                serde_json::json!({
                    "type": "Service",
                    "id": "https://relay.fedi.buzz/tag/activitypub",
                    "preferredUsername": "tag-activitypub",
                    "name": "#activitypub",
                    "inbox": "https://relay.fedi.buzz/tag/activitypub",
                    "outbox": "https://relay.fedi.buzz/tag/activitypub/outbox",
                    "endpoints": {
                        "sharedInbox": "https://relay.fedi.buzz/instance/relay.fedi.buzz"
                    }
                }),
            ),
            (
                GroupTarget::NodeBb,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://community.nodebb.org/category/30",
                    "inbox": "https://community.nodebb.org/category/30/inbox"
                }),
            ),
            (
                GroupTarget::Friendica,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://forum.friendi.ca/profile/developers",
                    "inbox": "https://forum.friendi.ca/inbox/developers"
                }),
            ),
            (
                GroupTarget::Hubzilla,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://hubzilla.example/channel/adminsforum",
                    "inbox": "https://hubzilla.example/inbox/adminsforum"
                }),
            ),
            (
                GroupTarget::WordPress,
                serde_json::json!({
                    "type": "Person",
                    "id": "https://blog.example/author/alice",
                    "inbox": "https://blog.example/author/alice/inbox"
                }),
            ),
            (
                GroupTarget::Funkwhale,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://music.example/channels/library",
                    "generator": {"name": "Funkwhale"},
                    "inbox": "https://music.example/channels/library/inbox"
                }),
            ),
            (
                GroupTarget::Bonfire,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design",
                    "generator": {"name": "Federation Bot"},
                    "inbox": "https://demo.bonfire.cafe/pub/actors/Bonfire_Design/inbox"
                }),
            ),
            (
                GroupTarget::Mobilizon,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://mobilizon.fr/@framasoft",
                    "inbox": "https://mobilizon.fr/@framasoft/inbox",
                    "outbox": "https://mobilizon.fr/@framasoft/outbox",
                    "members": "https://mobilizon.fr/@framasoft/members",
                    "resources": "https://mobilizon.fr/@framasoft/resources"
                }),
            ),
            (
                GroupTarget::Smithereen,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://friends.grishka.me/groups/example",
                    "inbox": "https://friends.grishka.me/groups/example/inbox",
                    "outbox": "https://friends.grishka.me/groups/example/outbox",
                    "wall": "https://friends.grishka.me/groups/example/wall"
                }),
            ),
            (
                GroupTarget::Discourse,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03",
                    "inbox": "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03/inbox",
                    "outbox": "https://meta.discourse.org/ap/actor/f510931b1c556bbc94ea1971a1924f03/outbox"
                }),
            ),
            (
                GroupTarget::WordPress,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://vivaldi.com/?author=0",
                    "preferredUsername": "blog",
                    "inbox": "https://vivaldi.com/wp-json/activitypub/1.0/actors/0/inbox",
                    "outbox": "https://vivaldi.com/wp-json/activitypub/1.0/actors/0/outbox"
                }),
            ),
            (
                GroupTarget::Flipboard,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://flipboard.com/@engadget/gear-nv6v86arz",
                    "generator": {"name": "Flipboard"},
                    "inbox": "https://flipboard.com/@engadget/gear-nv6v86arz/inbox"
                }),
            ),
            (
                GroupTarget::Elgg,
                serde_json::json!({
                    "type": "Group",
                    "id": "https://demo.wzm.me/activitypub/group/activitypubgroup",
                    "generator": {"name": "Elgg ActivityPub"},
                    "inbox": "https://demo.wzm.me/activitypub/group/activitypubgroup/inbox"
                }),
            ),
            (
                GroupTarget::Gancio,
                serde_json::json!({
                    "type": "Application",
                    "id": "https://gancio.cisti.org/federation/u/events",
                    "generator": {"name": "Gancio"},
                    "preferredUsername": "events",
                    "inbox": "https://gancio.cisti.org/federation/u/events/inbox"
                }),
            ),
            (
                GroupTarget::FediGroups,
                serde_json::json!({
                    "type": "Service",
                    "id": "https://fedigroups.social/groups/homelab",
                    "generator": {"name": "FediGroups"},
                    "inbox": "https://fedigroups.social/groups/homelab/inbox"
                }),
            ),
            (
                GroupTarget::FedibirdGroup,
                serde_json::json!({
                    "type": "Service",
                    "id": "https://gdev.fedibird.com/users/circledev",
                    "generator": {"name": "Fedibird Group Server"},
                    "inbox": "https://gdev.fedibird.com/users/circledev/inbox"
                }),
            ),
            (
                GroupTarget::GroupActor,
                serde_json::json!({
                    "type": "Service",
                    "id": "https://piggo.space/hob",
                    "generator": {"name": "group-actor"},
                    "inbox": "https://piggo.space/hob/inbox"
                }),
            ),
            (
                GroupTarget::WordPressEventBridge,
                serde_json::json!({
                    "type": "Application",
                    "id": "https://events.example/events",
                    "generator": {"name": "Event Bridge for ActivityPub"},
                    "inbox": "https://events.example/events/inbox"
                }),
            ),
        ];

        for (expected, value) in cases {
            let profile = classify_actor_value(&value);

            assert_eq!(profile.target, expected);
            assert_ne!(profile.family, GroupTargetFamily::Unknown);
            assert!(profile.has_inbox);
        }
    }

    #[test]
    fn known_object_classifier_ignores_summary_software_mentions() {
        let value = serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Group",
            "id": "https://diggita.com/c/opensource",
            "preferredUsername": "opensource",
            "inbox": "https://diggita.com/c/opensource/inbox",
            "outbox": "https://diggita.com/c/opensource/outbox",
            "followers": "https://diggita.com/c/opensource/followers",
            "generator": {"name": "Mastodon-compatible renderer"},
            "summary": "A group for open source people from mastodon.uno."
        });

        let object: crate::apub_util::KnownObject = serde_json::from_value(value).unwrap();
        let profile = classify_known_object(&object).unwrap();

        assert_eq!(profile.target, GroupTarget::Lemmy);
        assert_eq!(profile.family, GroupTargetFamily::ThreadiverseForum);
        assert_eq!(profile.actor_kind, TargetActorKind::Group);
    }

    #[test]
    fn actor_classifier_fails_safe_for_unsupported_shapes() {
        let person = classify_actor_value(&serde_json::json!({
            "type": "Person",
            "id": "https://example.com/users/alice"
        }));
        assert_eq!(person.target, GroupTarget::UnknownActor);
        assert_eq!(person.family, GroupTargetFamily::ProfileOnly);
        assert_eq!(person.actor_kind, TargetActorKind::Person);
        assert_eq!(
            person.support(FederationOperation::Follow),
            OperationSupport::Unsupported
        );

        let group_without_inbox = classify_actor_value(&serde_json::json!({
            "type": "Group",
            "id": "https://example.com/groups/no-inbox"
        }));
        assert_eq!(group_without_inbox.target, GroupTarget::UnknownGroup);
        assert_eq!(
            group_without_inbox.family,
            GroupTargetFamily::CollectionChannel
        );
        assert_eq!(
            group_without_inbox.support(FederationOperation::Follow),
            OperationSupport::Unsupported
        );
        assert_eq!(
            group_without_inbox.support(FederationOperation::ReceivePost),
            OperationSupport::Required
        );
    }

    #[test]
    fn wordpress_person_actor_with_plugin_endpoints_is_blog_publisher() {
        let profile = classify_actor_value(&serde_json::json!({
            "type": "Person",
            "id": "https://wedistribute.org/@news",
            "preferredUsername": "news",
            "name": "We Distribute",
            "inbox": "https://wedistribute.org/wp-json/activitypub/1.0/actors/0/inbox",
            "outbox": "https://wedistribute.org/wp-json/activitypub/1.0/actors/0/outbox",
            "followers": "https://wedistribute.org/wp-json/activitypub/1.0/actors/0/followers"
        }));

        assert_eq!(profile.target, GroupTarget::WordPress);
        assert_eq!(profile.family, GroupTargetFamily::BlogPublisher);
        assert_eq!(profile.actor_kind, TargetActorKind::Person);
        assert_eq!(
            profile.support(FederationOperation::Follow),
            OperationSupport::BestEffort
        );
        assert_eq!(
            profile.support(FederationOperation::PreviewHistory),
            OperationSupport::BestEffort
        );
    }

    #[test]
    fn unknown_followable_group_uses_collection_channel_fallback() {
        let profile = classify_actor_value(&serde_json::json!({
            "type": "Group",
            "id": "https://unknown.example/groups/radio",
            "inbox": "https://unknown.example/groups/radio/inbox",
            "outbox": "https://unknown.example/groups/radio/outbox",
            "followers": "https://unknown.example/groups/radio/followers"
        }));

        assert_eq!(profile.target, GroupTarget::UnknownGroup);
        assert_eq!(profile.family, GroupTargetFamily::CollectionChannel);
        assert_eq!(profile.source(), "heuristic");
        assert_eq!(
            profile.support(FederationOperation::Follow),
            OperationSupport::Required
        );
        assert_eq!(
            profile.support(FederationOperation::Comment),
            OperationSupport::BestEffort
        );
    }

    #[test]
    fn unknown_followable_service_uses_relay_fallback() {
        let profile = classify_actor_value(&serde_json::json!({
            "type": "Service",
            "id": "https://relay.example/groups/homelab",
            "inbox": "https://relay.example/groups/homelab/inbox",
            "followers": "https://relay.example/groups/homelab/followers"
        }));

        assert_eq!(profile.target, GroupTarget::UnknownActor);
        assert_eq!(profile.family, GroupTargetFamily::RelayBot);
        assert_eq!(
            profile.support(FederationOperation::Follow),
            OperationSupport::Required
        );
        assert_eq!(
            profile.support(FederationOperation::Like),
            OperationSupport::Unsupported
        );
    }

    #[test]
    fn unknown_person_stays_profile_only() {
        let profile = classify_actor_value(&serde_json::json!({
            "type": "Person",
            "id": "https://social.example/users/alice",
            "inbox": "https://social.example/users/alice/inbox",
            "outbox": "https://social.example/users/alice/outbox"
        }));

        assert_eq!(profile.target, GroupTarget::UnknownActor);
        assert_eq!(profile.family, GroupTargetFamily::ProfileOnly);
        assert_eq!(
            profile.support(FederationOperation::Follow),
            OperationSupport::Required
        );
        assert_eq!(
            profile.support(FederationOperation::CreatePost),
            OperationSupport::Unsupported
        );
    }
}

/* end of target.rs */
