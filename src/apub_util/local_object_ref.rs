use super::{ApIdRef, try_strip_host};
use crate::BaseURL;
use crate::types::{
    CollectionTargetLocalID, CommentLocalID, CommunityLocalID, PollLocalID, PollOptionLocalID,
    PostLocalID, UserLocalID,
};

type RefRouteNode<P> = trout::Node<P, String, LocalObjectRef, ()>;

lazy_static::lazy_static! {
    static ref LOCAL_REF_ROUTES: RefRouteNode<()> = {
        RefRouteNode::new()
            .with_child(
                "comments",
                RefRouteNode::new()
                    .with_child_parse::<CommentLocalID, _>(
                        RefRouteNode::new().with_handler((), |(comment,), (), _| LocalObjectRef::Comment(comment))
                            .with_child(
                                "likes",
                                RefRouteNode::new()
                                    .with_handler((), |(comment,), (), _| LocalObjectRef::CommentLikes(comment))
                                    .with_child_parse::<UserLocalID, _>(RefRouteNode::new().with_handler((), |(comment, user), (), _| LocalObjectRef::CommentLike(comment, user)))
                            )
                    )
            )
            .with_child(
                "collection_targets",
                RefRouteNode::new()
                    .with_child_parse::<CollectionTargetLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(target,), (), _| LocalObjectRef::CollectionTarget(target))
                            .with_child(
                                "followers",
                                RefRouteNode::new()
                                    .with_handler((), |(target,), (), _| LocalObjectRef::CollectionTargetFollowers(target))
                                    .with_child_parse::<UserLocalID, _>(
                                        RefRouteNode::new().with_handler((), |(target, follower), (), _| LocalObjectRef::CollectionTargetFollow(target, follower))
                                    )
                            )
                    )
            )
            .with_child(
                "communities",
                RefRouteNode::new()
                    .with_child_parse::<CommunityLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(community,), (), _| LocalObjectRef::Community(community))
                            .with_child(
                                "featured",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), (), _| LocalObjectRef::CommunityFeatured(community))
                            )
                            .with_child(
                                "followers",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), (), _| LocalObjectRef::CommunityFollowers(community))
                                    .with_child_parse::<UserLocalID, _>(
                                        RefRouteNode::new()
                                            .with_handler((), |(community, follower), (), _| LocalObjectRef::CommunityFollow(community, follower))
                                            .with_child(
                                                "join",
                                                RefRouteNode::new()
                                                    .with_handler((), |(community, follower), (), _| LocalObjectRef::CommunityFollowJoin(community, follower))
                                            )
                                    )
                            )
                            .with_child(
                                "inbox",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), (), _| LocalObjectRef::CommunityInbox(community))
                            )
                            .with_child(
                                "outbox",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), (), _| LocalObjectRef::CommunityOutbox(community))
                                    .with_child("page", RefRouteNode::new().with_child_parse::<crate::TimestampOrLatest, _>(RefRouteNode::new().with_handler((), |(community, page), (), _| LocalObjectRef::CommunityOutboxPage(community, page))))
                            )
                    )
            )
            .with_child("inbox", RefRouteNode::new().with_handler((), |(), (), _| LocalObjectRef::SharedInbox))
            .with_child("polls", RefRouteNode::new().with_child_parse::<PollLocalID, _>(
                    RefRouteNode::new().with_child(
                        "voters",
                        RefRouteNode::new().with_child_parse::<UserLocalID, _>(
                            RefRouteNode::new().with_child(
                                "votes",
                                RefRouteNode::new().with_child_parse::<PollOptionLocalID, _>(
                                    RefRouteNode::new().with_handler((), |(poll, user, option), (), _| LocalObjectRef::PollVote(poll, user, option))))))))
            .with_child(
                "posts",
                RefRouteNode::new()
                    .with_child_parse::<PostLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(post,), (), _| LocalObjectRef::Post(post))
                            .with_child(
                                "likes",
                                RefRouteNode::new()
                                    .with_handler((), |(post,), (), _| LocalObjectRef::PostLikes(post))
                                    .with_child_parse::<UserLocalID, _>(RefRouteNode::new().with_handler((), |(post, user), (), _| LocalObjectRef::PostLike(post, user)))
                            )
                    )
            )
            .with_child(
                "users",
                RefRouteNode::new()
                    .with_child_parse::<UserLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(user,), (), _| LocalObjectRef::User(user))
                            .with_child(
                                "followers",
                                RefRouteNode::new()
                                    .with_handler(
                                        (),
                                        |(user,), (), _| LocalObjectRef::UserFollowers(user),
                                    )
                                    .with_child_parse::<UserLocalID, _>(
                                        RefRouteNode::new()
                                            .with_handler(
                                                (),
                                                |(user, follower), (), _| {
                                                    LocalObjectRef::UserFollow(user, follower)
                                                },
                                            )
                                            .with_child(
                                                "join",
                                                RefRouteNode::new().with_handler(
                                                    (),
                                                    |(user, follower), (), _| {
                                                        LocalObjectRef::UserFollowJoin(
                                                            user, follower,
                                                        )
                                                    },
                                                ),
                                            ),
                                    ),
                            )
                            .with_child("following", RefRouteNode::new().with_handler((), |(user,), (), _| LocalObjectRef::UserFollowing(user)))
                            .with_child("liked", RefRouteNode::new().with_handler((), |(user,), (), _| LocalObjectRef::UserLiked(user)))
                            .with_child("outbox", RefRouteNode::new().with_handler((), |(user,), (), _| LocalObjectRef::UserOutbox(user)).with_child("page", RefRouteNode::new().with_child_parse::<crate::TimestampOrLatest, _>(RefRouteNode::new().with_handler((), |(user, page), (), _| LocalObjectRef::UserOutboxPage(user, page)))))
                    )
            )
    };
}

#[derive(Debug, Clone, Copy)]
pub enum LocalObjectRef {
    Comment(CommentLocalID),
    CommentLikes(CommentLocalID),
    CommentLike(CommentLocalID, UserLocalID),
    CollectionTarget(CollectionTargetLocalID),
    CollectionTargetFollowers(CollectionTargetLocalID),
    CollectionTargetFollow(CollectionTargetLocalID, UserLocalID),
    Community(CommunityLocalID),
    CommunityFeatured(CommunityLocalID),
    CommunityFollowers(CommunityLocalID),
    CommunityFollow(CommunityLocalID, UserLocalID),
    CommunityFollowJoin(CommunityLocalID, UserLocalID),
    CommunityInbox(CommunityLocalID),
    CommunityOutbox(CommunityLocalID),
    CommunityOutboxPage(CommunityLocalID, crate::TimestampOrLatest),
    PollVote(PollLocalID, UserLocalID, PollOptionLocalID),
    Post(PostLocalID),
    PostLikes(PostLocalID),
    PostLike(PostLocalID, UserLocalID),
    SharedInbox,
    User(UserLocalID),
    UserFollowers(UserLocalID),
    UserFollowing(UserLocalID),
    UserFollow(UserLocalID, UserLocalID),
    UserFollowJoin(UserLocalID, UserLocalID),
    UserLiked(UserLocalID),
    UserOutbox(UserLocalID),
    UserOutboxPage(UserLocalID, crate::TimestampOrLatest),
}

impl LocalObjectRef {
    pub fn try_from_path(path: &str) -> Option<LocalObjectRef> {
        if !path.starts_with('/') {
            return None;
        }

        let path = path[1..].to_owned();
        log::debug!("checking local object {path}");
        let res = LOCAL_REF_ROUTES.route(path, ());
        log::debug!("found {res:?}");
        res.ok()
    }

    pub fn try_from_uri(
        uri: &(impl ApIdRef + ?Sized),
        host_url_apub: &BaseURL,
    ) -> Option<LocalObjectRef> {
        if let Some(remaining) = try_strip_host(uri, host_url_apub) {
            LocalObjectRef::try_from_path(remaining)
        } else {
            None
        }
    }

    pub fn to_local_uri(self, host_url_apub: &BaseURL) -> BaseURL {
        match self {
            LocalObjectRef::Comment(comment) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["comments", &comment.to_string()]);
                res
            }
            LocalObjectRef::CommentLikes(comment) => {
                let mut res = LocalObjectRef::Comment(comment).to_local_uri(host_url_apub);
                res.path_segments_mut().push("likes");
                res
            }
            LocalObjectRef::CommentLike(comment, user) => {
                let mut res = LocalObjectRef::CommentLikes(comment).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&user.to_string());
                res
            }
            LocalObjectRef::CollectionTarget(target) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["collection_targets", &target.to_string()]);
                res
            }
            LocalObjectRef::CollectionTargetFollowers(target) => {
                let mut res = LocalObjectRef::CollectionTarget(target).to_local_uri(host_url_apub);
                res.path_segments_mut().push("followers");
                res
            }
            LocalObjectRef::CollectionTargetFollow(target, follower) => {
                let mut res =
                    LocalObjectRef::CollectionTargetFollowers(target).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&follower.to_string());
                res
            }
            LocalObjectRef::Community(community) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["communities", &community.to_string()]);
                res
            }
            LocalObjectRef::CommunityFeatured(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("featured");
                res
            }
            LocalObjectRef::CommunityFollowers(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("followers");
                res
            }
            LocalObjectRef::CommunityFollow(community, follower) => {
                let mut res =
                    LocalObjectRef::CommunityFollowers(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&follower.to_string());
                res
            }
            LocalObjectRef::CommunityFollowJoin(community, follower) => {
                let mut res = LocalObjectRef::CommunityFollow(community, follower)
                    .to_local_uri(host_url_apub);
                res.path_segments_mut().push("join");
                res
            }
            LocalObjectRef::CommunityInbox(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("inbox");
                res
            }
            LocalObjectRef::CommunityOutbox(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("outbox");
                res
            }
            LocalObjectRef::CommunityOutboxPage(community, page) => {
                let mut res =
                    LocalObjectRef::CommunityOutbox(community).to_local_uri(host_url_apub);
                res.path_segments_mut().extend(&["page", &page.to_string()]);
                res
            }
            LocalObjectRef::PollVote(poll, user, option) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut().extend(&[
                    "polls",
                    &poll.to_string(),
                    "voters",
                    &user.to_string(),
                    "votes",
                    &option.to_string(),
                ]);
                res
            }
            LocalObjectRef::Post(post) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["posts", &post.to_string()]);
                res
            }
            LocalObjectRef::PostLikes(post) => {
                let mut res = LocalObjectRef::Post(post).to_local_uri(host_url_apub);
                res.path_segments_mut().push("likes");
                res
            }
            LocalObjectRef::PostLike(post, user) => {
                let mut res = LocalObjectRef::PostLikes(post).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&user.to_string());
                res
            }
            LocalObjectRef::SharedInbox => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut().push("inbox");
                res
            }
            LocalObjectRef::User(user) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["users", &user.to_string()]);
                res
            }
            LocalObjectRef::UserFollowers(user) => {
                let mut res = LocalObjectRef::User(user).to_local_uri(host_url_apub);
                res.path_segments_mut().push("followers");
                res
            }
            LocalObjectRef::UserFollowing(user) => {
                let mut res = LocalObjectRef::User(user).to_local_uri(host_url_apub);
                res.path_segments_mut().push("following");
                res
            }
            LocalObjectRef::UserFollow(target_user, follower) => {
                let mut res =
                    LocalObjectRef::UserFollowers(target_user).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&follower.to_string());
                res
            }
            LocalObjectRef::UserFollowJoin(target_user, follower) => {
                let mut res =
                    LocalObjectRef::UserFollow(target_user, follower).to_local_uri(host_url_apub);
                res.path_segments_mut().push("join");
                res
            }
            LocalObjectRef::UserLiked(user) => {
                let mut res = LocalObjectRef::User(user).to_local_uri(host_url_apub);
                res.path_segments_mut().push("liked");
                res
            }
            LocalObjectRef::UserOutbox(user) => {
                let mut res = LocalObjectRef::User(user).to_local_uri(host_url_apub);
                res.path_segments_mut().push("outbox");
                res
            }
            LocalObjectRef::UserOutboxPage(user, page) => {
                let mut res = LocalObjectRef::UserOutbox(user).to_local_uri(host_url_apub);
                res.path_segments_mut().extend(&["page", &page.to_string()]);
                res
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_activitypub_collection_refs_round_trip() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();
        let user = UserLocalID(42);

        let following = LocalObjectRef::UserFollowing(user).to_local_uri(&host_url_apub);
        assert_eq!(
            following.as_str(),
            "https://lotide.example/apub/users/42/following"
        );
        assert!(matches!(
            LocalObjectRef::try_from_uri(&following, &host_url_apub),
            Some(LocalObjectRef::UserFollowing(UserLocalID(42)))
        ));

        let liked = LocalObjectRef::UserLiked(user).to_local_uri(&host_url_apub);
        assert_eq!(liked.as_str(), "https://lotide.example/apub/users/42/liked");
        assert!(matches!(
            LocalObjectRef::try_from_uri(&liked, &host_url_apub),
            Some(LocalObjectRef::UserLiked(UserLocalID(42)))
        ));
    }

    #[test]
    fn object_like_collection_refs_round_trip() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();

        let post_likes = LocalObjectRef::PostLikes(PostLocalID(77)).to_local_uri(&host_url_apub);
        assert_eq!(
            post_likes.as_str(),
            "https://lotide.example/apub/posts/77/likes"
        );
        assert!(matches!(
            LocalObjectRef::try_from_uri(&post_likes, &host_url_apub),
            Some(LocalObjectRef::PostLikes(PostLocalID(77)))
        ));

        let comment_likes =
            LocalObjectRef::CommentLikes(CommentLocalID(88)).to_local_uri(&host_url_apub);
        assert_eq!(
            comment_likes.as_str(),
            "https://lotide.example/apub/comments/88/likes"
        );
        assert!(matches!(
            LocalObjectRef::try_from_uri(&comment_likes, &host_url_apub),
            Some(LocalObjectRef::CommentLikes(CommentLocalID(88)))
        ));
    }

    #[test]
    fn collection_target_follow_refs_round_trip() {
        let host_url_apub: crate::BaseURL = "https://lotide.example/apub".parse().unwrap();

        let follow =
            LocalObjectRef::CollectionTargetFollow(CollectionTargetLocalID(15), UserLocalID(7))
                .to_local_uri(&host_url_apub);

        assert_eq!(
            follow.as_str(),
            "https://lotide.example/apub/collection_targets/15/followers/7"
        );
        assert!(matches!(
            LocalObjectRef::try_from_uri(&follow, &host_url_apub),
            Some(LocalObjectRef::CollectionTargetFollow(
                CollectionTargetLocalID(15),
                UserLocalID(7)
            ))
        ));
    }
}
