# Unreleased

- Lotide local fork: align the extension crate with the vendored
  `activitystreams` `0.8.0-alpha.25` baseline.
- Keep the existing extension wrapper API while allowing Lotide's local
  ActivityStreams conformance tests to exercise extension-bearing documents.
- Fix example in readme.
- Clean up extension wrapper construction order for stricter Clippy coverage.
- Clean up crate metadata and verify the vendored fork with rustdoc warnings
  denied and doctests enabled.

# 0.1.0-alpha.2
Fix docs

# 0.1.0-alpha.1
Add `.into_parts()` for ext types

# 0.1.0-alpha.0
Initial release
