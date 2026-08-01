use std::fmt;

use crate::DocumentMetadata;

/// Decodes typed document metadata from a Markdown byte sequence.
///
/// The YAML decoder rejects aliases, anchors, merge keys, multiple documents,
/// and inputs that exceed the configured front-matter byte limit.
///
/// # Errors
///
/// Returns a deterministic syntax or size failure when the Markdown does not
/// contain one bounded YAML front-matter document.
pub fn decode_document_metadata(
    markdown: &[u8],
    maximum_front_matter_bytes: usize,
) -> Result<DocumentMetadata, DocumentParseError> {
    let markdown = std::str::from_utf8(markdown).map_err(|_| DocumentParseError::InvalidUtf8)?;
    let yaml = extract_front_matter(markdown, maximum_front_matter_bytes)?;
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: maximum_front_matter_bytes,
            max_aliases: 0,
            max_anchors: 0,
            max_depth: 16,
            max_documents: 1,
            max_nodes: maximum_front_matter_bytes,
            max_total_scalar_bytes: maximum_front_matter_bytes,
            max_total_comment_bytes: maximum_front_matter_bytes,
            max_merge_keys: 0,
        },
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        strict_booleans: true,
        with_snippet: false,
    };
    serde_saphyr::from_str_with_options(yaml, options)
        .map_err(|source| DocumentParseError::InvalidYaml(Box::new(source)))
}

/// Returns the Markdown body after the required YAML front matter.
///
/// # Errors
///
/// Returns an error when the bytes are not UTF-8 or the required front-matter
/// delimiters are missing.
pub fn markdown_body(markdown: &[u8]) -> Result<&str, DocumentParseError> {
    let markdown = std::str::from_utf8(markdown).map_err(|_| DocumentParseError::InvalidUtf8)?;
    let remainder = markdown
        .strip_prefix("---\n")
        .or_else(|| markdown.strip_prefix("---\r\n"))
        .ok_or(DocumentParseError::MissingOpeningDelimiter)?;
    let mut body_offset = 0_usize;
    for line in remainder.split_inclusive('\n') {
        let without_newline = line.strip_suffix('\n').unwrap_or(line);
        let content = without_newline
            .strip_suffix('\r')
            .unwrap_or(without_newline);
        body_offset += line.len();
        if content == "---" {
            return Ok(&remainder[body_offset..]);
        }
    }
    Err(DocumentParseError::MissingClosingDelimiter)
}

fn extract_front_matter(markdown: &str, maximum_bytes: usize) -> Result<&str, DocumentParseError> {
    let remainder = markdown
        .strip_prefix("---\n")
        .or_else(|| markdown.strip_prefix("---\r\n"))
        .ok_or(DocumentParseError::MissingOpeningDelimiter)?;

    let mut offset = 0_usize;
    for line in remainder.split_inclusive('\n') {
        let without_newline = line.strip_suffix('\n').unwrap_or(line);
        let content = without_newline
            .strip_suffix('\r')
            .unwrap_or(without_newline);
        if content == "---" {
            if offset > maximum_bytes {
                return Err(DocumentParseError::FrontMatterTooLarge {
                    maximum: maximum_bytes,
                    actual: offset,
                });
            }
            return Ok(&remainder[..offset]);
        }
        offset += line.len();
        if offset > maximum_bytes {
            return Err(DocumentParseError::FrontMatterTooLarge {
                maximum: maximum_bytes,
                actual: offset,
            });
        }
    }

    Err(DocumentParseError::MissingClosingDelimiter)
}

/// A bounded Markdown front-matter decoding failure.
#[derive(Debug)]
pub enum DocumentParseError {
    /// The Markdown bytes were not valid UTF-8.
    InvalidUtf8,
    /// The Markdown did not start with `---`.
    MissingOpeningDelimiter,
    /// The YAML front matter had no closing `---`.
    MissingClosingDelimiter,
    /// The front matter exceeded the configured byte limit.
    FrontMatterTooLarge {
        /// Configured maximum front-matter bytes.
        maximum: usize,
        /// Observed bytes before decoding stopped.
        actual: usize,
    },
    /// The YAML document was malformed or exceeded a parser budget.
    InvalidYaml(Box<serde_saphyr::Error>),
}

impl fmt::Display for DocumentParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUtf8 => formatter.write_str("Markdown is not UTF-8"),
            Self::MissingOpeningDelimiter => {
                formatter.write_str("Markdown must start with a YAML front-matter delimiter")
            }
            Self::MissingClosingDelimiter => {
                formatter.write_str("Markdown has no closing YAML front-matter delimiter")
            }
            Self::FrontMatterTooLarge { maximum, actual } => write!(
                formatter,
                "front matter is {actual} bytes; configured maximum is {maximum}"
            ),
            Self::InvalidYaml(source) => write!(formatter, "invalid YAML: {source}"),
        }
    }
}

impl std::error::Error for DocumentParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidYaml(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DocumentParseError, extract_front_matter, markdown_body};

    #[test]
    fn extracts_lf_and_crlf_front_matter() {
        assert!(matches!(
            extract_front_matter("---\ntitle: Fictional\n---\nBody\n", 1024),
            Ok("title: Fictional\n")
        ));
        assert!(matches!(
            extract_front_matter("---\r\ntitle: Fictional\r\n---\r\nBody\r\n", 1024),
            Ok("title: Fictional\r\n")
        ));
    }

    #[test]
    fn rejects_missing_front_matter_delimiters() {
        assert!(matches!(
            extract_front_matter("title: Fictional\n", 1024),
            Err(DocumentParseError::MissingOpeningDelimiter)
        ));
        assert!(matches!(
            extract_front_matter("---\ntitle: Fictional\n", 1024),
            Err(DocumentParseError::MissingClosingDelimiter)
        ));
    }

    #[test]
    fn rejects_front_matter_over_the_byte_limit() {
        assert!(matches!(
            extract_front_matter("---\ntitle: Fictional\n---\n", 4),
            Err(DocumentParseError::FrontMatterTooLarge {
                maximum: 4,
                actual: 17
            })
        ));
    }

    #[test]
    fn returns_only_the_markdown_body_for_lf_and_crlf_documents() {
        assert!(matches!(
            markdown_body(b"---\ntitle: Fictional\n---\nBody\n"),
            Ok("Body\n")
        ));
        assert!(matches!(
            markdown_body(b"---\r\ntitle: Fictional\r\n---\r\nBody\r\n"),
            Ok("Body\r\n")
        ));
    }
}
