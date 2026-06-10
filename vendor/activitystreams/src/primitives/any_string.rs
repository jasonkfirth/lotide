use crate::primitives::{Either, OneOrMany, RdfLangString};
use std::collections::BTreeMap;

/// A type representing any kind of string
///
/// In the ActivityStreams specification, string types are often defined as either an xsd:String or
/// and rdf:langString. The AnyString type represents this union.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum AnyStringValue {
    Xsd(String),
    Rdf(RdfLangString),
    LanguageMap(BTreeMap<String, String>),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct AnyString(AnyStringValue);

impl AnyString {
    /// Borrow the AnyString as an &str
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::AnyString;
    /// # let any_string = AnyString::from_xsd_string("hi");
    /// #
    /// let s_borrow = any_string
    ///     .as_xsd_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_xsd_string(&self) -> Option<&str> {
        match self.0 {
            AnyStringValue::Xsd(ref string) => Some(string),
            _ => None,
        }
    }

    /// Borrow the AnyString as an RdfLangString
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{AnyString, RdfLangString};
    /// # let any_string = AnyString::from_rdf_lang_string(RdfLangString {
    /// #     value: "hi".into(),
    /// #     language: "en".into(),
    /// # });
    /// #
    /// let s_borrow = any_string
    ///     .as_rdf_lang_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_rdf_lang_string(&self) -> Option<&RdfLangString> {
        match self.0 {
            AnyStringValue::Rdf(ref string) => Some(string),
            _ => None,
        }
    }

    /// Borrow a non-standard language map.
    ///
    /// Activity Streams 2.0 represents language maps using sibling properties
    /// such as `nameMap`. Some deployed software and older test fixtures send
    /// the map directly as `name`. The parser accepts that shape so callers can
    /// recover content from otherwise useful objects, while strict conformance
    /// checks can still reject it.
    pub fn as_language_map(&self) -> Option<&BTreeMap<String, String>> {
        match self.0 {
            AnyStringValue::LanguageMap(ref map) => Some(map),
            _ => None,
        }
    }

    /// Take the AnyString as a String
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::AnyString;
    /// # let any_string = AnyString::from_xsd_string("hi");
    /// #
    /// let xsd_string = any_string
    ///     .xsd_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn xsd_string(self) -> Option<String> {
        match self.0 {
            AnyStringValue::Xsd(string) => Some(string),
            _ => None,
        }
    }

    /// Take the AnyString as an RdfLangString
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{AnyString, RdfLangString};
    /// # let any_string = AnyString::from_rdf_lang_string(RdfLangString {
    /// #     value: "hi".into(),
    /// #     language: "en".into(),
    /// # });
    /// #
    /// let rdf_lang_string = any_string
    ///     .rdf_lang_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rdf_lang_string(self) -> Option<RdfLangString> {
        match self.0 {
            AnyStringValue::Rdf(string) => Some(string),
            _ => None,
        }
    }

    /// Take a non-standard language map.
    pub fn language_map(self) -> Option<BTreeMap<String, String>> {
        match self.0 {
            AnyStringValue::LanguageMap(map) => Some(map),
            _ => None,
        }
    }

    /// Create a new AnyString from an `Into<String>`
    ///
    /// ```rust
    /// use activitystreams::primitives::AnyString;
    ///
    /// let any_string = AnyString::from_xsd_string("hi");
    /// ```
    pub fn from_xsd_string<T>(string: T) -> Self
    where
        T: Into<String>,
    {
        AnyString(AnyStringValue::Xsd(string.into()))
    }

    /// Create a new AnyString from an RdfLangString
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, RdfLangString};
    ///
    /// let any_string = AnyString::from_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    /// ```
    pub fn from_rdf_lang_string<T>(string: T) -> Self
    where
        T: Into<RdfLangString>,
    {
        AnyString(AnyStringValue::Rdf(string.into()))
    }

    /// Create a new AnyString from a non-standard language map.
    pub fn from_language_map<T>(map: T) -> Self
    where
        T: Into<BTreeMap<String, String>>,
    {
        AnyString(AnyStringValue::LanguageMap(map.into()))
    }

    /// Replace the contents of self with a String
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, RdfLangString};
    ///
    /// let mut any_string = AnyString::from_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    ///
    /// any_string.set_xsd_string("hi");
    ///
    /// assert!(any_string.as_xsd_string().is_some());
    /// ```
    pub fn set_xsd_string<T>(&mut self, string: T)
    where
        T: Into<String>,
    {
        self.0 = AnyStringValue::Xsd(string.into());
    }

    /// Replace the contents of self with an RdfLangString
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, RdfLangString};
    ///
    /// let mut any_string = AnyString::from_xsd_string("hi");
    ///
    /// any_string.set_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    ///
    /// assert!(any_string.as_rdf_lang_string().is_some());
    /// ```
    pub fn set_rdf_lang_string<T>(&mut self, string: T)
    where
        T: Into<RdfLangString>,
    {
        self.0 = AnyStringValue::Rdf(string.into());
    }

    /// Replace the contents with a non-standard language map.
    pub fn set_language_map<T>(&mut self, map: T)
    where
        T: Into<BTreeMap<String, String>>,
    {
        self.0 = AnyStringValue::LanguageMap(map.into());
    }

    /// Borrow the inner str
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, RdfLangString};
    /// let any_string = AnyString::from_xsd_string("hi");
    ///
    /// assert_eq!(any_string.as_str(), "hi");
    ///
    /// let any_string = AnyString::from_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    ///
    /// assert_eq!(any_string.as_str(), "hi");
    /// ```
    pub fn as_str(&self) -> &str {
        match self.0 {
            AnyStringValue::Xsd(ref string) => string,
            AnyStringValue::Rdf(ref lang_str) => &lang_str.value,
            AnyStringValue::LanguageMap(ref map) => {
                map.values().next().map(String::as_str).unwrap_or_default()
            }
        }
    }

    /// Borrow the inner language
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, RdfLangString};
    /// let any_string = AnyString::from_xsd_string("hi");
    ///
    /// assert_eq!(any_string.language(), None);
    ///
    /// let any_string = AnyString::from_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    ///
    /// assert_eq!(any_string.language(), Some("en"));
    /// ```
    pub fn language(&self) -> Option<&str> {
        match self.0 {
            AnyStringValue::Xsd(_) => None,
            AnyStringValue::Rdf(ref lang_str) => Some(&lang_str.language),
            AnyStringValue::LanguageMap(ref map) => map.keys().next().map(String::as_str),
        }
    }
}

impl AsRef<str> for AnyString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl OneOrMany<AnyString> {
    /// Try to borrow a single String from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, AnyString};
    /// # let string = OneOrMany::<AnyString>::from_xsd_string("Hey");
    /// string
    ///     .as_single_xsd_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_single_xsd_string(&self) -> Option<&str> {
        self.as_one()
            .and_then(|any_string| any_string.as_xsd_string())
    }

    /// Try to borrow a single RdfLangString from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, RdfLangString};
    /// # let string = OneOrMany::from_rdf_lang_string(RdfLangString {
    /// #   value: "hi".into(),
    /// #   language: "en".into(),
    /// # });
    /// string
    ///     .as_single_rdf_lang_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_single_rdf_lang_string(&self) -> Option<&RdfLangString> {
        self.as_one()
            .and_then(|any_string| any_string.as_rdf_lang_string())
    }

    /// Try to take a single String from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, AnyString};
    /// # let string = OneOrMany::<AnyString>::from_xsd_string("Hey");
    /// string
    ///     .single_xsd_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn single_xsd_string(self) -> Option<String> {
        self.one().and_then(|any_string| any_string.xsd_string())
    }

    /// Try to take a single RdfLangString from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, RdfLangString};
    /// # let string = OneOrMany::from_rdf_lang_string(RdfLangString {
    /// #   value: "hi".into(),
    /// #   language: "en".into(),
    /// # });
    /// string
    ///     .single_rdf_lang_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn single_rdf_lang_string(self) -> Option<RdfLangString> {
        self.one()
            .and_then(|any_string| any_string.rdf_lang_string())
    }

    /// Create the object from a single String
    ///
    /// ```rust
    /// use activitystreams::primitives::{OneOrMany, AnyString};
    ///
    /// let string = OneOrMany::<AnyString>::from_xsd_string("hi");
    /// ```
    pub fn from_xsd_string<T>(string: T) -> Self
    where
        T: Into<String>,
    {
        Self::from_one(AnyString::from_xsd_string(string))
    }

    /// Create the object from a single RdfLangString
    ///
    /// ```rust
    /// use activitystreams::primitives::{OneOrMany, RdfLangString};
    ///
    /// let string = OneOrMany::from_rdf_lang_string(RdfLangString {
    ///     value: "hi".into(),
    ///     language: "en".into(),
    /// });
    /// ```
    pub fn from_rdf_lang_string<T>(string: T) -> Self
    where
        T: Into<RdfLangString>,
    {
        Self::from_one(AnyString::from_rdf_lang_string(string))
    }

    /// Add a String to the object, appending to whatever is currently included
    ///
    /// ```rust
    /// use activitystreams::primitives::{OneOrMany, AnyString};
    ///
    /// let mut string = OneOrMany::<AnyString>::from_xsd_string("Hello");
    ///
    /// string
    ///     .add_xsd_string("Hey")
    ///     .add_xsd_string("hi");
    /// ```
    pub fn add_xsd_string<T>(&mut self, string: T) -> &mut Self
    where
        T: Into<String>,
    {
        self.add(string.into())
    }

    /// Add an RdfLangString to the object, appending to whatever is currently included
    ///
    /// ```rust
    /// use activitystreams::primitives::{AnyString, OneOrMany, RdfLangString};
    ///
    /// let mut string = OneOrMany::<AnyString>::from_xsd_string("Hello");
    ///
    /// string
    ///     .add_rdf_lang_string(RdfLangString {
    ///         value: "Hey".into(),
    ///         language: "en".into(),
    ///     })
    ///     .add_rdf_lang_string(RdfLangString {
    ///         value: "hi".into(),
    ///         language: "en".into(),
    ///     });
    /// ```
    pub fn add_rdf_lang_string<T>(&mut self, string: T) -> &mut Self
    where
        T: Into<RdfLangString>,
    {
        self.add(string.into())
    }
}

impl OneOrMany<&AnyString> {
    /// Try to borrow a single String from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, AnyString};
    /// # let string = OneOrMany::<AnyString>::from_xsd_string("Hey");
    /// string
    ///     .as_single_xsd_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_single_xsd_string(&self) -> Option<&str> {
        self.as_one()
            .and_then(|any_string| any_string.as_xsd_string())
    }

    /// Try to borrow a single RdfLangString from the current object
    ///
    /// ```rust
    /// # fn main() -> Result<(), anyhow::Error> {
    /// # use activitystreams::primitives::{OneOrMany, RdfLangString};
    /// # let string = OneOrMany::from_rdf_lang_string(RdfLangString {
    /// #   value: "hi".into(),
    /// #   language: "en".into(),
    /// # });
    /// string
    ///     .as_single_rdf_lang_string()
    ///     .ok_or(anyhow::Error::msg("Wrong string type"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_single_rdf_lang_string(&self) -> Option<&RdfLangString> {
        self.as_one()
            .and_then(|any_string| any_string.as_rdf_lang_string())
    }

    /// Create and owned clone of the OneOrMany<AnyString>
    ///
    /// ```rust
    /// # use activitystreams::primitives::{OneOrMany, AnyString};
    /// # let string = OneOrMany::<AnyString>::from_xsd_string("hey");
    ///
    /// let borrowed_string: OneOrMany<&AnyString> = string.as_ref();
    ///
    /// let owned_one_or_many: OneOrMany<AnyString> = borrowed_string.to_owned();
    /// ```
    pub fn to_owned(self) -> OneOrMany<AnyString> {
        match self.0 {
            Either::Left([one_ref]) => OneOrMany(Either::Left([one_ref.to_owned()])),
            Either::Right(many_ref) => {
                OneOrMany(Either::Right(many_ref.into_iter().cloned().collect()))
            }
        }
    }
}

impl From<&str> for AnyString {
    fn from(s: &str) -> Self {
        AnyString::from_xsd_string(s.to_owned())
    }
}

impl From<String> for AnyString {
    fn from(s: String) -> Self {
        AnyString::from_xsd_string(s)
    }
}

impl From<RdfLangString> for AnyString {
    fn from(s: RdfLangString) -> Self {
        AnyString::from_rdf_lang_string(s)
    }
}

impl From<&str> for OneOrMany<AnyString> {
    fn from(s: &str) -> Self {
        OneOrMany::<AnyString>::from_xsd_string(s.to_owned())
    }
}

impl From<String> for OneOrMany<AnyString> {
    fn from(s: String) -> Self {
        OneOrMany::<AnyString>::from_xsd_string(s)
    }
}

impl From<RdfLangString> for OneOrMany<AnyString> {
    fn from(s: RdfLangString) -> Self {
        OneOrMany::<AnyString>::from_rdf_lang_string(s)
    }
}
