# ActivityPub Live Audit 2026-06-05

File: activitypub-live-audit-20260605.md

Purpose:

    Record the empirical compatibility audit performed against the current
    ActivityPub platform target matrix.

Responsibilities:

    - separate passive remote ActivityPub reachability from Lotide behavior
    - record the actual public and actor URLs tested
    - identify which failures need code, fixture, or target-candidate work
    - leave future developers enough context to reproduce the audit

This file intentionally does NOT contain:

    - private credentials
    - remote server passwords
    - destructive maintenance commands
    - claims of signed delivery success where only passive reads were tested

## Method

The audit used two local tools:

- `activitypub_matrix_probe_20260605.py`
- `lotide_live_lookup_audit_20260605.py`

The passive probe fetched WebFinger, actor JSON, outbox, featured, and a small
sample of collection objects. The Lotide probe then used the public
`/api/unstable/actors:lookup` route and read the resolved community or user,
post list, first post replies, and first post votes.

The quiet read-only pass did not create remote posts or comments and did not
send signed likes, follows, or unfollows.

## Result Summary

| Platform | Target tested | Passive ActivityPub | Lotide lookup | Preview/posts | Comments | Result |
| --- | --- | --- | --- | --- | --- | --- |
| Lotide | `announcements@dev.narwhal.city` | Group actor, outbox has `Announce` and `Add` | community `39` | 0 current visible posts | n/a | Needs a current active Lotide peer for routine audit. Existing content appears older than retention. |
| Lemmy | `opensource@diggita.com` | Group actor, outbox has announces | community `4347735` | posts imported | first selected post had no replies | Pass for read/preview. |
| Lemmy | `gaming@lemmy.zip` | Group actor, outbox has announces | community `736` | posts imported | first selected post had no replies | Pass for read/preview. |
| PieFed | `ask@piefed.social` | Group actor, outbox has announces | community `4350302` | posts imported | first selected post had no replies | Pass for read/preview. |
| PieFed | `historymemes@piefed.social` | Group actor, outbox has announces | community `4249465` | posts imported | first selected post had no replies | Pass for read/preview. |
| Kbin | no current live target | n/a | n/a | n/a | n/a | Public Kbin instance lists were stale in the June 2026 retest. Candidates were dead, parked, Cloudflare-blocked, or running Mbin. Keep true Kbin covered by fixtures until a real actor is found. |
| Mbin/Kbin legacy | `https://kbin.earth/m/random` | Group actor, featured has embedded item, WebFinger 404, public outbox empty | community `4434595` | posts imported through Mbin/Kbin API fallback | some replies in DB | Read/preview pass. The host appears in kbin-named lists but is discovered as Mbin-compatible. Signed magazine inbox delivery hit Cloudflare challenge HTML even after fallback transport. |
| Mbin | `https://thebrainbin.org/m/AskMbin` | Group actor, outbox empty, featured/API useful | community `4356864` | posts imported | comments imported | Pass for the Mbin API fallback. |
| Mbin | `updates@kbin.melroy.org` | Group actor, empty outbox/featured | community `4347736` | 0 posts | n/a | Bad preview candidate. Use AskMbin or another active magazine. |
| NodeBB | `https://forums.ubports.com/category/8/off-topic` | category actor and category outbox are ActivityPub-capable | community `4401443` | posts imported through NodeBB outbox/API fallback | replies imported in DB | Pass for NodeBB category fallback. |
| NodeBB | `activitypub@community.nodebb.org` | Group actor, outbox has announces | community `4347814` | posts imported after worker drain | not repeated in this pass | Pass for read import after worker drain. Keep the fixture coverage because this host is a useful non-UBports NodeBB shape. |
| Discourse ActivityPub | `feature@meta.discourse.org` | Group actor under `/ap/actor/...` | community `4434601` | one post imported | selected post had no replies | Partial pass. Classifier was patched to identify this as Discourse. |
| Discourse ActivityPub | `announcements@meta.discourse.org` | Group actor, outbox returned 500, category JSON works | community `4434600` | posts imported through topic-list fallback | comments imported when topic JSON exposes replies | Pass for Discourse category fallback after requeue on the current binary. |
| Friendica | `helpers@forum.friendi.ca` | Group actor, old AP outbox plus Atom fallback | community `4347764` | posts imported | replies imported | Pass for current Friendica fallback. |
| Friendica | `admins@forum.friendi.ca` | Group actor | community `4434603` | posts imported | replies imported | Pass for current Friendica fallback. |
| PeerTube | `fediforum_demos@spectra.video` | Group actor, outbox has video announces | community `4358624` | videos imported | comments imported | Pass for read/preview. |
| PeerTube | `blender_studio@video.blender.org` | Group actor, outbox has video announces | community `4350114` | videos imported | comments exist in DB | Pass for read/preview. |
| Mobilizon | `framasoft@mobilizon.fr` | Group actor, outbox has `Create` activities | community `4347763` | event posts imported after worker drain | n/a | Pass for event read import. `Create{Event}` wrappers now have explicit regression coverage. |
| Hubzilla | `adminsforum@hubzilla.org` | Group actor, outbox has `Add` wrappers | community `4347765` | posts imported | replies imported | Pass for current Hubzilla `Add` wrapper support. |
| Streams/Forte | `linuxuserspace@podcastindex.social` | WebFinger and public URL 404 | lookup 404 | n/a | n/a | Bad candidate. Find a current Streams/Forte channel. |
| Bonfire | `Bonfire_Design@demo.bonfire.cafe` | Group actor, outbox total 0 | community `4403406` | 0 posts | n/a | Actor support passes, but this candidate has no preview content. |
| Bonfire | `Demo_group_1@demo.bonfire.cafe` | Group actor, outbox has announces | community `4434609` | posts imported | no replies | Better Bonfire read candidate. |
| Flipboard | `engadget@flipboard.com` and magazine URL | WebFinger resolves a Person actor; magazine URL not actor JSON | user only | n/a | n/a | Code/product gap. Keep user actor separate from magazine URLs. |
| Elgg | `activitypubgroup@demo.wzm.me` | Group actor, outbox has announces/create | community `4347767` | posts imported after worker drain | not repeated in this pass | Pass for read import after worker drain. Demo instance remains slow, so fixture coverage is more reliable than depending on it for every test run. |
| Gancio | `gancio@gancio.cisti.org` | Application actor, outbox has events | community `4451442` | posts imported | no replies expected for event previews | Pass after deployed Application-as-community routing. |
| Funkwhale | `https://tanukitunes.com/federation/music/libraries/2ac0f854-cc34-40c5-a98e-2bda535a9134` | public `Library` object with owner actor, not a normal group actor | collection target lookup implemented after audit | preview crawler implemented after audit | n/a | Lotide stores Funkwhale libraries as collection targets, sends follows to the owner actor inbox, and stores a bounded preview of Audio items from the library page. |
| FediGroups | `homelab@fedigroups.social` | Service actor, Mastodon-style outbox announces object URLs | community `4451449` | posts imported after bounded relay preview fetch | replies best-effort from announced objects | Pass for relay read/preview with capped relay object fetches. |
| BuzzRelay | `https://relay.fedi.buzz/tag/activitypub` | Service actor with generated tag inbox and outbox | community `4806425` | empty outbox before follow/use | n/a | Actor resolves and now classifies as `BuzzRelay` relay bot. Treat as opt-in relay, not a broad community-directory source. |
| Fedibird Group | `playground@gdev.fedibird.com`, `circledev@gdev.fedibird.com` | Group actor, outbox has announces | community `4802932` / older `4347762` | posts imported | comments imported | Preview, Like, and Undo Like pass against announced statuses. The live server sends signed `Reject` for Follow, so this is a remote policy exception until an accepting Fedibird group is found. |
| Group Actor | `hob@piggo.space` | Service actor, outbox has announces | community `4451454` | posts imported | no replies in sample | Pass after deployed Service-as-community routing. |
| WordPress ActivityPub | `blog@vivaldi.com` | Group actor at `https://vivaldi.com/?author=0` | community `4347766` | posts imported | not checked | Read pass. Classifier now recognizes WordPress actor paths. |
| WordPress ActivityPub | redacted WordPress author actor | Person author actor with WordPress ActivityPub routes | community `4591381` | post imported through explicit lookup | target post advertised no replies | Read pass after WordPress blog-publisher actor fix. Signed Like reached the inbox but was rejected while WordPress fetched Lotide's signing-key profile. |
| WordPress Event Bridge | no live event actor | n/a | n/a | n/a | n/a | Needs a live actor or fixture. |
| Mastodon/Pleroma/Akkoma | profile-only inbound follow class | n/a | n/a | n/a | n/a | Not a group provider. Test inbound follows separately. |
| Smithereen | no live group actor | n/a | n/a | n/a | n/a | Needs fixtures or a supplied actor. |
| AP-Groups/chirp | no current chirp.social target | n/a | n/a | n/a | n/a | Use FediGroups for live relay behavior. |
| Old Guppe | no safe `a.gup.pe` target | n/a | n/a | n/a | n/a | Use FediGroups for live relay behavior. |

## Code Follow-Up From This Audit

Changes made after the audit:

- `hot_rank` now clamps age and score inputs so future-dated remote posts cannot
  make the public feed return HTTP 500.
- The target classifier recognizes live Discourse `/ap/actor/...` actors.
- The target classifier recognizes Mobilizon actors by group collections such
  as `members`, `events`, and `resources`.
- The target classifier recognizes WordPress ActivityPub actors that use
  `?author=` actor IDs or `/wp-json/activitypub/` inbox/outbox paths.
- Group-like `Service` and `Application` actors now flow through the remote
  community insert path instead of always becoming person-like users.
- WordPress blog-publisher `Person` actors now become group-like targets during
  explicit lookup when they own top-level posts. Author-backed WordPress blogs
  can then import posts under a remote community row instead of dropping them
  for lack of a threadiverse community actor.
- Local Person and Group actor documents now include an ActivityStreams `url`
  field alongside `id` and `publicKey`. Some WordPress-style actor importers
  use that field while fetching profiles and signing keys.

Verification after deployment on the live Lotide host:

- `/api/unstable/instance` and `/api/unstable/posts?limit=1` returned HTTP 200.
- `gancio@gancio.cisti.org` resolved as community `4451442` and imported event
  posts.
- `homelab@fedigroups.social` resolved as community `4451449` and imported relay
  posts and replies.
- `hob@piggo.space` resolved as community `4451454` and imported relay posts.
- `blog@vivaldi.com` remained a community and its WordPress actor target was
  identified as `WordPress`.
- A WordPress author actor resolved as community `4591381`, and the WordPress
  permalink imported as post `530577`.
- A signed Like to the WordPress inbox failed with
  `activitypub_signature_verification: No Profile found or Profile not accessible`.
  The same failure reproduced with a cache-busted `keyId` and an `acct:` key ID,
  so the blocker appears to be WordPress' remote fetch of the Lotide actor/key
  profile rather than the Like activity shape. Public DNS for the tested Lotide host
  points at a public address, while the Lotide server's own resolver maps the
  same host to private reverse-proxy addresses; matching resolver behavior on
  the WordPress host would cause WordPress `wp_safe_remote_get` to reject the
  actor URL as unsafe.

Still open:

- Flipboard magazine pages advertise an ActivityPub alternate link to
  `/magazines/...`. Actor lookup already follows that link; object lookup now
  uses the same alternate-link fallback so public magazine URLs do not collapse
  to the profile actor or fail as plain HTML.
- Funkwhale has live public library targets. They are modeled as
  `collection_target` rows that store the followed `Library` object separately
  from the owner actor inbox that receives the `Follow`. A bounded preview
  crawler now stores recent Audio items from the library page.
- Streams/Forte still needs a current group or forum actor. A direct probe of
  `macgirvin.com/channel/mike` found a Person actor, not a group candidate.
- WordPress Event Bridge, Smithereen, AP-Groups/chirp, and old Guppe still need
  live actors for signed delivery. Source-backed parser coverage now exists for
  Event Bridge-style Application actors, Smithereen-style wall groups, and
  generic relay/group actor shapes. The Event Federation site exposes a
  WordPress blog actor, but no current event bridge actor was found in this
  pass.
- Signed follow/unfollow/like/unlike was not repeated in this pass. Use the
  read results here to choose a quiet target per platform before running signed
  delivery probes.

<!-- end of activitypub-live-audit-20260605.md -->
