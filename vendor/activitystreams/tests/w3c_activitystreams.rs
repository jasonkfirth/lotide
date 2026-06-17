/*
    Project: Lotide vendored ActivityStreams library
    ------------------------------------------------

    File: w3c_activitystreams.rs

    Purpose:

        Exercise the vendored ActivityStreams parser against the W3C
        Activity Streams 2.0 JSON fixture corpus.

    Responsibilities:

        - load the W3C JSON fixtures vendored under tests/w3c
        - verify that every non-fail fixture parses as an ActivityStreams value
        - keep the standards corpus wired into cargo test

    This file intentionally does NOT contain:

        - Lotide federation behavior tests
        - network access to the upstream W3C repository
        - ActivityPub protocol delivery tests
*/

use activitystreams::base::AnyBase;
use activitystreams::conformance;
use std::path::{Path, PathBuf};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("w3c")
}

fn collect_json_files(path: &Path, output: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(path).expect("fixture directory must be readable") {
        let entry = entry.expect("fixture entry must be readable");
        let path = entry.path();

        if path.is_dir() {
            collect_json_files(&path, output);
        } else if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("json" | "jsonldb")
        ) {
            output.push(path);
        }
    }
}

fn relative_fixture_name(path: &Path) -> String {
    let root = fixture_root();
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn documented_positive_fixture_deviation(name: &str) -> Option<&'static str> {
    match name.replace('\\', "/").as_str() {
        "simple0011.json" | "simple0012.json" => {
            Some("old corpus language-map form uses name as an object instead of nameMap")
        }
        "vocabulary-ex196-jsonld.json" => {
            Some("fixture contains unescaped raw newlines inside a JSON string")
        }
        _ => None,
    }
}

#[test]
fn w3c_non_fail_fixtures_parse_as_activitystreams_documents() {
    let root = fixture_root();
    let fail_root = root.join("fail");
    let mut paths = Vec::new();

    collect_json_files(&root, &mut paths);
    paths.sort();

    let mut failures = Vec::new();

    for path in paths {
        if path.starts_with(&fail_root) {
            continue;
        }

        let name = relative_fixture_name(&path);
        if documented_positive_fixture_deviation(&name).is_some() {
            continue;
        }

        let fixture = std::fs::read_to_string(&path).expect("fixture must be readable");

        if let Err(error) = serde_json::from_str::<AnyBase>(&fixture) {
            failures.push(format!("{name}: {error}"));
        }
    }

    assert!(
        failures.is_empty(),
        "W3C ActivityStreams fixtures failed to parse:\n{}",
        failures.join("\n")
    );
}

#[test]
fn w3c_non_fail_fixtures_validate_under_strict_conformance_rules() {
    let root = fixture_root();
    let fail_root = root.join("fail");
    let mut paths = Vec::new();

    collect_json_files(&root, &mut paths);
    paths.sort();

    let mut failures = Vec::new();

    for path in paths {
        if path.starts_with(&fail_root) {
            continue;
        }

        let name = relative_fixture_name(&path);
        if documented_positive_fixture_deviation(&name).is_some() {
            continue;
        }

        let fixture = std::fs::read_to_string(&path).expect("fixture must be readable");

        if let Err(error) = conformance::validate_activitystreams_json_str(&fixture) {
            failures.push(format!("{name}: {error}"));
        }
    }

    assert!(
        failures.is_empty(),
        "W3C ActivityStreams fixtures failed strict validation:\n{}",
        failures.join("\n")
    );
}

#[test]
fn w3c_fail_fixtures_are_rejected_by_strict_conformance_rules() {
    let fail_root = fixture_root().join("fail");
    let mut paths = Vec::new();

    collect_json_files(&fail_root, &mut paths);
    paths.sort();

    let mut failures = Vec::new();

    for path in paths {
        let name = relative_fixture_name(&path);
        let bytes = std::fs::read(&path).expect("fixture must be readable");
        let Ok(fixture) = std::str::from_utf8(&bytes) else {
            continue;
        };

        if conformance::validate_activitystreams_json_str(fixture).is_ok() {
            failures.push(name);
        }
    }

    assert!(
        failures.is_empty(),
        "W3C ActivityStreams fail fixtures unexpectedly validated:\n{}",
        failures.join("\n")
    );
}

#[test]
fn documented_positive_fixture_deviations_are_rejected_by_strict_rules() {
    let root = fixture_root();
    let mut failures = Vec::new();

    for name in [
        "simple0011.json",
        "simple0012.json",
        "vocabulary-ex196-jsonld.json",
    ] {
        let fixture = std::fs::read(root.join(name)).expect("fixture must be readable");
        let result = std::str::from_utf8(&fixture)
            .map_err(|error| error.to_string())
            .and_then(|fixture| {
                conformance::validate_activitystreams_json_str(fixture)
                    .map_err(|error| error.to_string())
            });

        if result.is_ok() {
            failures.push(name);
        }
    }

    assert!(
        failures.is_empty(),
        "documented W3C fixture deviations unexpectedly validated:\n{}",
        failures.join("\n")
    );
}

/* end of w3c_activitystreams.rs */
