# ActivityPub Conformance

Lotide aims to be an ActivityPub federated server first. Compatibility with
deployed group software is still the deciding factor when real servers differ
from the recommendation, but those deviations need to be documented and tested.

Baseline reference:

- ActivityPub, W3C Recommendation, 23 January 2018:
  `https://www.w3.org/TR/activitypub/`

## Current Profile

Lotide implements the server-to-server federation profile. The client-to-server
outbox profile is not exposed as a general third-party API; Hitide remains the
supported local user interface.

The conformance goal for local ActivityPub objects is:

- local actors publish `inbox` and `outbox`
- local actors publish `followers`
- local user actors publish count-only `following` and `liked` collections
- actor and collection GET responses use ActivityStreams context and
  `application/activity+json`
- activities sent over federation use globally dereferenceable local ids
- delivery workers do not expose private delivery fields such as `bto` or `bcc`
  in browser-facing output
- inbound objects are signature checked and then normalized through the
  platform compatibility layer before persistence

## Intentional Minimal Implementations

The recommendation allows `following` and `liked` collections on actors. Lotide
now exposes them for local user actors as count-only `Collection` objects. This
is intentionally minimal because publishing full membership lists can expose
social graph detail that the old UI never promised to make public.

Follower collections are also count-only. The counts include accepted follows
only. Pending follows are protocol state, not followers.

## Test Expectations

Unit tests should cover the shape of locally generated ActivityPub documents
even when the live federation behavior is exercised manually. At minimum, tests
should pin down:

- local object id routes for every local ActivityPub object
- actor documents containing required and implemented optional links
- collection responses containing `@context`, `type`, `id`, and `totalItems`
- outgoing `Follow`, `Undo`, `Create`, `Like`, and `Accept` shapes
- compatibility-specific addressing required by Lemmy, PieFed, Mbin, PeerTube,
  Friendica, Hubzilla, NodeBB, Discourse, Bonfire, and relay-style group actors
- rejection or quarantine behavior for malformed or hostile inbound activities

## Compatibility Deviations

ActivityPub says servers should validate inbound objects by dereferencing their
origin when practical. Lotide verifies signed delivery and applies extra
fetch/cross-check behavior where the platform layer knows how to do it. It does
not block all objects that cannot be re-fetched immediately because several
real group platforms are slow, temporarily private, or use non-standard outbox
pagination. These exceptions belong in
`activitypub-platform-compatibility.md`.

Lotide does not expose a general ActivityPub client-to-server API. This is a
product boundary, not a federation limitation. Any future implementation should
reuse the same object builders used by Hitide so outgoing federation shapes stay
identical.
