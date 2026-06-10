/*
    Project: ActivityStreams conformance support
    --------------------------------------------

    File: conformance.rs

    Purpose:

        Validate JSON documents against the parts of Activity Streams 2.0
        that can be checked without JSON-LD expansion.

    Responsibilities:

        - reject non-object ActivityStreams document roots
        - check common ActivityStreams property ranges
        - check the W3C known-bad fixture cases used by the test corpus
        - keep strict validation separate from tolerant federation parsing

    This file intentionally does NOT contain:

        - ActivityPub delivery validation
        - remote dereferencing of IRIs
        - full JSON-LD expansion or compaction
*/

use iri_string::types::IriStr;
use serde_json::{Map, Value};

const ACTIVITYSTREAMS_CONTEXT_HTTP: &str = "http://www.w3.org/ns/activitystreams";
const ACTIVITYSTREAMS_CONTEXT_HTTPS: &str = "https://www.w3.org/ns/activitystreams";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConformanceError {
    path: String,
    message: String,
}

impl ConformanceError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        ConformanceError {
            path: path.into(),
            message: message.into(),
        }
    }

    /// Location within the JSON document where validation failed.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Human-readable validation failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for ConformanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for ConformanceError {}

/// Parse and validate a JSON ActivityStreams document.
///
/// This function intentionally uses strict JSON parsing. Activity Streams 2.0
/// documents are JSON documents, so inputs with unescaped control characters
/// are rejected before semantic validation begins.
pub fn validate_activitystreams_json_str(input: &str) -> Result<(), ConformanceError> {
    let value = serde_json::from_str(input)
        .map_err(|error| ConformanceError::new("$", format!("invalid JSON: {}", error)))?;

    validate_activitystreams_document(&value)
}

/// Validate an already parsed ActivityStreams document.
pub fn validate_activitystreams_document(value: &Value) -> Result<(), ConformanceError> {
    let object = value.as_object().ok_or_else(|| {
        ConformanceError::new("$", "ActivityStreams document root must be a JSON object")
    })?;

    validate_object(object, "$")
}

fn validate_object(object: &Map<String, Value>, path: &str) -> Result<(), ConformanceError> {
    if let Some(context) = object.get("@context") {
        validate_context(context, &child_path(path, "@context"))?;
    }

    if let Some(kind) = object.get("type").or_else(|| object.get("@type")) {
        validate_type(kind, &child_path(path, "type"))?;
    }

    if let Some(id) = object.get("id") {
        validate_absolute_iri_value(id, &child_path(path, "id"))?;
    }

    for key in ["name", "summary", "content"] {
        if let Some(value) = object.get(key) {
            validate_natural_language_value(value, &child_path(path, key))?;
        }
    }

    for key in ["nameMap", "summaryMap", "contentMap"] {
        if let Some(value) = object.get(key) {
            validate_language_map(value, &child_path(path, key))?;
        }
    }

    if let Some(href) = object.get("href") {
        validate_absolute_iri_value(href, &child_path(path, "href"))?;
    }

    if let Some(url) = object.get("url") {
        validate_iri_or_object_value(url, &child_path(path, "url"))?;
    }

    for &key in object_or_link_properties() {
        if let Some(value) = object.get(key) {
            validate_object_or_link_value(value, &child_path(path, key))?;
        }
    }

    validate_collection_shape(object, path)?;

    for (key, value) in object {
        if key == "@context" {
            continue;
        }

        validate_nested_value(value, &child_path(path, key))?;
    }

    Ok(())
}

fn validate_nested_value(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_nested_value(value, &index_path(path, index))?;
            }

            Ok(())
        }
        Value::Object(object) => validate_object(object, path),
        _ => Ok(()),
    }
}

fn validate_context(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) | Value::Array(_) | Value::Object(_) => {
            if context_contains_activitystreams(value) {
                Ok(())
            } else {
                Err(ConformanceError::new(
                    path,
                    "context must include the ActivityStreams namespace",
                ))
            }
        }
        _ => Err(ConformanceError::new(
            path,
            "context must be a string, object, array, or null",
        )),
    }
}

fn context_contains_activitystreams(value: &Value) -> bool {
    match value {
        Value::String(value) => is_activitystreams_context(value),
        Value::Array(values) => values.iter().any(context_contains_activitystreams),
        Value::Object(object) => object
            .get("@vocab")
            .and_then(Value::as_str)
            .map(is_activitystreams_context)
            .unwrap_or(false),
        _ => false,
    }
}

fn is_activitystreams_context(value: &str) -> bool {
    let value = value.trim_end_matches('#');

    value == ACTIVITYSTREAMS_CONTEXT_HTTP || value == ACTIVITYSTREAMS_CONTEXT_HTTPS
}

fn validate_type(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) => Ok(()),
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                if !value.is_string() {
                    return Err(ConformanceError::new(
                        index_path(path, index),
                        "type array entries must be strings",
                    ));
                }
            }

            Ok(())
        }
        _ => Err(ConformanceError::new(
            path,
            "type must be a string, string array, or null",
        )),
    }
}

fn validate_absolute_iri_value(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(value) if is_absolute_iri(value) => Ok(()),
        Value::String(_) => Err(ConformanceError::new(path, "value must be an absolute IRI")),
        _ => Err(ConformanceError::new(
            path,
            "value must be an absolute IRI string or null",
        )),
    }
}

fn validate_iri_or_object_value(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) => validate_absolute_iri_value(value, path),
        Value::Object(object) => validate_object(object, path),
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_iri_or_object_value(value, &index_path(path, index))?;
            }

            Ok(())
        }
        _ => Err(ConformanceError::new(
            path,
            "value must be an IRI string, object, array, or null",
        )),
    }
}

fn validate_object_or_link_value(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) => validate_absolute_iri_value(value, path),
        Value::Object(object) => validate_object(object, path),
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_object_or_link_value(value, &index_path(path, index))?;
            }

            Ok(())
        }
        _ => Err(ConformanceError::new(
            path,
            "value must be an Object, Link, IRI string, array, or null",
        )),
    }
}

fn validate_natural_language_value(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) => Ok(()),
        Value::Object(object) => validate_rdf_lang_string(object, path),
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_natural_language_value(value, &index_path(path, index))?;
            }

            Ok(())
        }
        _ => Err(ConformanceError::new(
            path,
            "natural language value must be a string, rdf:langString, array, or null",
        )),
    }
}

fn validate_rdf_lang_string(
    object: &Map<String, Value>,
    path: &str,
) -> Result<(), ConformanceError> {
    match object.get("@value") {
        Some(Value::String(_)) => {}
        _ => {
            return Err(ConformanceError::new(
                path,
                "language maps must use the matching *Map property",
            ));
        }
    }

    if let Some(language) = object.get("@language") {
        match language.as_str() {
            Some(language) if language_tag_is_valid(language) => {}
            Some(_) => {
                return Err(ConformanceError::new(
                    child_path(path, "@language"),
                    "language tag is invalid",
                ));
            }
            None => {
                return Err(ConformanceError::new(
                    child_path(path, "@language"),
                    "language tag must be a string",
                ));
            }
        }
    }

    Ok(())
}

fn validate_language_map(value: &Value, path: &str) -> Result<(), ConformanceError> {
    let object = match value {
        Value::Null => return Ok(()),
        Value::Object(object) => object,
        _ => {
            return Err(ConformanceError::new(
                path,
                "language map must be an object or null",
            ));
        }
    };

    for (language, value) in object {
        if !language_tag_is_valid(language) {
            return Err(ConformanceError::new(
                child_path(path, language),
                "language tag is invalid",
            ));
        }

        if !value.is_string() {
            return Err(ConformanceError::new(
                child_path(path, language),
                "language map values must be strings",
            ));
        }
    }

    Ok(())
}

fn validate_collection_shape(
    object: &Map<String, Value>,
    path: &str,
) -> Result<(), ConformanceError> {
    if type_includes(object, "OrderedCollection") && object.contains_key("items") {
        return Err(ConformanceError::new(
            child_path(path, "items"),
            "OrderedCollection must use orderedItems",
        ));
    }

    if type_includes(object, "Collection") && object.contains_key("orderedItems") {
        return Err(ConformanceError::new(
            child_path(path, "orderedItems"),
            "Collection must use items",
        ));
    }

    for key in ["current", "first", "last"] {
        if let Some(value) = object.get(key) {
            validate_collection_page_reference(value, &child_path(path, key))?;
        }
    }

    Ok(())
}

fn validate_collection_page_reference(value: &Value, path: &str) -> Result<(), ConformanceError> {
    match value {
        Value::Null => Ok(()),
        Value::String(_) => validate_absolute_iri_value(value, path),
        Value::Object(object) => {
            if type_includes(object, "Link")
                || type_includes(object, "CollectionPage")
                || type_includes(object, "OrderedCollectionPage")
            {
                validate_object(object, path)
            } else {
                Err(ConformanceError::new(
                    path,
                    "collection paging properties must point to CollectionPage or Link",
                ))
            }
        }
        _ => Err(ConformanceError::new(
            path,
            "collection paging property must be an IRI string, object, or null",
        )),
    }
}

fn object_or_link_properties() -> &'static [&'static str] {
    &[
        "actor",
        "object",
        "target",
        "origin",
        "result",
        "instrument",
        "attachment",
        "attributedTo",
        "audience",
        "preview",
        "generator",
        "icon",
        "image",
        "location",
        "tag",
        "inReplyTo",
        "replies",
        "to",
        "bto",
        "cc",
        "bcc",
        "items",
        "orderedItems",
        "oneOf",
        "anyOf",
        "partOf",
        "next",
        "prev",
    ]
}

fn type_includes(object: &Map<String, Value>, expected: &str) -> bool {
    object
        .get("type")
        .or_else(|| object.get("@type"))
        .map(|value| value_type_includes(value, expected))
        .unwrap_or(false)
}

fn value_type_includes(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    }
}

fn is_absolute_iri(value: &str) -> bool {
    IriStr::new(value).is_ok()
}

fn language_tag_is_valid(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(language) = parts.next() else {
        return false;
    };

    if !is_alpha_len(language, 2, 8) {
        return false;
    }

    let mut script_seen = false;
    let mut region_seen = false;

    for part in parts {
        if part.is_empty() {
            return false;
        }

        if is_alpha_len(part, 4, 4) {
            if script_seen || region_seen {
                return false;
            }

            script_seen = true;
        } else if is_region_subtag(part) {
            if region_seen {
                return false;
            }

            region_seen = true;
        } else if !is_alphanumeric_len(part, 5, 8) {
            return false;
        }
    }

    true
}

fn is_region_subtag(value: &str) -> bool {
    is_alpha_len(value, 2, 2) || is_digit_len(value, 3, 3)
}

fn is_alpha_len(value: &str, min: usize, max: usize) -> bool {
    value.len() >= min && value.len() <= max && value.bytes().all(|byte| byte.is_ascii_alphabetic())
}

fn is_digit_len(value: &str, min: usize, max: usize) -> bool {
    value.len() >= min && value.len() <= max && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_alphanumeric_len(value: &str, min: usize, max: usize) -> bool {
    value.len() >= min
        && value.len() <= max
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn child_path(path: &str, child: &str) -> String {
    format!("{}.{}", path, child)
}

fn index_path(path: &str, index: usize) -> String {
    format!("{}[{}]", path, index)
}

/* end of conformance.rs */
