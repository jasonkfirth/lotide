# ActivityStreams Conformance

Lotide vendors a local copy of `activitystreams` because upstream maintenance is
uncertain and federation compatibility requires controlled fixes.

Baseline references:

- Activity Streams 2.0 Core, W3C Recommendation, 23 May 2017:
  `https://www.w3.org/TR/activitystreams-core/`
- Activity Streams 2.0 Vocabulary, W3C Recommendation, 23 May 2017:
  `https://www.w3.org/TR/activitystreams-vocabulary/`
- W3C ActivityStreams test corpus:
  `https://github.com/w3c/activitystreams/tree/master/test`

The vendored test corpus was copied from W3C repository commit
`97b74b05d25da5c497dcaabd45b83f37865bdc72`.

## Implementation Model

The `activitystreams` parser remains tolerant. It accepts compact JSON objects,
preserves unknown fields through `Unparsed`, and accepts some non-standard input
that appears in older test fixtures or real servers.

Strict checks live in `activitystreams::conformance`. That module validates the
JSON shape and common ActivityStreams property ranges without fetching remote
IRIs or performing full JSON-LD expansion.

This separation is deliberate:

- federation ingestion should recover useful objects where possible
- tests and defensive callers can reject known-bad documents explicitly
- compatibility exceptions are visible instead of hidden in ad hoc parsing code

## W3C Corpus Coverage

The Rust test `vendor/activitystreams/tests/w3c_activitystreams.rs` wires the
W3C corpus into `cargo test`.

It verifies:

- every non-`fail` JSON fixture, except documented corpus deviations below,
  parses as `activitystreams::base::AnyBase`
- every non-`fail` JSON fixture, except documented corpus deviations below,
  passes `activitystreams::conformance`
- every fixture under `test/fail` is rejected by strict conformance checks or by
  strict JSON/UTF-8 decoding

## Compatibility Deviations

### Direct language maps on natural-language fields

Strict Activity Streams 2.0 uses `nameMap`, `summaryMap`, and `contentMap` for
language maps. Older fixtures and some real systems use direct maps such as:

```json
{
  "name": {
    "en": "Title"
  }
}
```

The tolerant parser accepts this shape by preserving it as an `AnyString`
language map. `activitystreams::conformance` rejects it unless the matching
`*Map` property is used.

The W3C corpus contains this contradiction:

- `simple0011.json` and `simple0012.json` use direct `name` maps
- `fail/namemap-as-name.json` says that direct `name` maps are invalid

Lotide prioritizes compatibility for ingestion and strictness for validation.

### ActivityStreams context with trailing fragment

Some older corpus fixtures use:

```json
"@context": "http://www.w3.org/ns/activitystreams#"
```

Strict validation accepts this as an ActivityStreams namespace alias. It still
rejects unrelated contexts such as `http://schema.org`.

### Invalid JSON fixture

`vocabulary-ex196-jsonld.json` contains literal newlines inside a JSON string.
Strict JSON parsers reject that before ActivityStreams validation can run.

Lotide keeps rejecting that fixture. Accepting it would require a non-JSON
preprocessor and would weaken the JSON boundary for remote federation input.

## Known Limit

The vendored crate validates compact ActivityStreams JSON. It does not perform
full JSON-LD expansion, remote context loading, RDF graph comparison, or remote
IRI dereferencing. Lotide's ActivityPub support depends on the compact JSON
form used by deployed federation software.
