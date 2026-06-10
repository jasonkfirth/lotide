lazy_static::lazy_static! {
    // This is more restrictive than the WebFinger spec but probably okay?
    pub static ref MENTION_REGEX: regex::Regex = regex::Regex::new(r"@([A-Za-z0-9-_~.]+)@([A-Za-z0-9-_~.]+(?::[0-9]+)?)").unwrap();
    static ref URL_REGEX: regex::Regex = regex::Regex::new(r"https?://[^\s<>()]+").unwrap();
}

pub fn parse_markdown(src: &str) -> impl Iterator<Item = pulldown_cmark::Event<'_>> {
    let parser = pulldown_cmark::Parser::new(src);

    linkify_bare_urls(parser).into_iter()
}

pub fn render_markdown_from_stream<'a>(
    stream: impl Iterator<Item = pulldown_cmark::Event<'a>>,
) -> String {
    let mut output = String::new();
    pulldown_cmark::html::push_html(&mut output, stream);

    output
}

pub fn render_markdown_simple(src: &str) -> String {
    render_markdown_from_stream(parse_markdown(src))
}

fn linkify_bare_urls<'a>(
    stream: impl Iterator<Item = pulldown_cmark::Event<'a>>,
) -> Vec<pulldown_cmark::Event<'a>> {
    let mut result = Vec::new();
    let mut link_depth = 0usize;

    for event in stream {
        match event {
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::Link { .. }) => {
                link_depth = link_depth.saturating_add(1);
                result.push(event);
            }
            pulldown_cmark::Event::End(pulldown_cmark::TagEnd::Link) => {
                link_depth = link_depth.saturating_sub(1);
                result.push(event);
            }
            pulldown_cmark::Event::Text(text) if link_depth == 0 => {
                append_linkified_text(&mut result, &text);
            }
            other => result.push(other),
        }
    }

    result
}

fn append_linkified_text(result: &mut Vec<pulldown_cmark::Event<'_>>, text: &str) {
    let mut covered = 0;

    for url_match in URL_REGEX.find_iter(text) {
        if covered < url_match.start() {
            result.push(pulldown_cmark::Event::Text(
                text[covered..url_match.start()].to_owned().into(),
            ));
        }

        let raw_url = url_match.as_str();
        let url_len = raw_url
            .trim_end_matches(['.', ',', '!', '?', ':', ';'])
            .len();
        let url = &raw_url[..url_len];
        let trailing = &raw_url[url_len..];

        if url.is_empty() {
            result.push(pulldown_cmark::Event::Text(raw_url.to_owned().into()));
        } else {
            let tag = pulldown_cmark::Tag::Link {
                link_type: pulldown_cmark::LinkType::Inline,
                dest_url: url.to_owned().into(),
                title: "".into(),
                id: "".into(),
            };
            result.push(pulldown_cmark::Event::Start(tag.clone()));
            result.push(pulldown_cmark::Event::Text(url.to_owned().into()));
            result.push(pulldown_cmark::Event::End(tag.to_end()));

            if !trailing.is_empty() {
                result.push(pulldown_cmark::Event::Text(trailing.to_owned().into()));
            }
        }

        covered = url_match.end();
    }

    if covered == 0 {
        result.push(pulldown_cmark::Event::Text(text.to_owned().into()));
    } else if covered < text.len() {
        result.push(pulldown_cmark::Event::Text(
            text[covered..].to_owned().into(),
        ));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn render_markdown_simple_linkifies_bare_urls() {
        let rendered = super::render_markdown_simple("See https://example.com/path.");

        assert_eq!(
            rendered,
            "<p>See <a href=\"https://example.com/path\">https://example.com/path</a>.</p>\n"
        );
    }

    #[test]
    fn render_markdown_simple_does_not_rewrite_existing_links() {
        let rendered = super::render_markdown_simple("[site](https://example.com/path)");

        assert_eq!(
            rendered,
            "<p><a href=\"https://example.com/path\">site</a></p>\n"
        );
    }
}
