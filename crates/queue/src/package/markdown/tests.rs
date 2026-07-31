use agent_knowledge_core::PayloadPath;

use super::{MarkdownValidationError, extract_front_matter};

fn path() -> PayloadPath {
    match "fictional/index.md".parse() {
        Ok(path) => path,
        Err(error) => panic!("fixture path must parse: {error}"),
    }
}

#[test]
fn extracts_lf_and_crlf_front_matter() {
    let lf = match extract_front_matter("---\ntitle: Fictional\n---\nBody\n", &path(), 1024) {
        Ok(yaml) => yaml,
        Err(error) => panic!("LF front matter must parse: {error}"),
    };
    assert_eq!(lf, "title: Fictional\n");

    let crlf =
        match extract_front_matter("---\r\ntitle: Fictional\r\n---\r\nBody\r\n", &path(), 1024) {
            Ok(yaml) => yaml,
            Err(error) => panic!("CRLF front matter must parse: {error}"),
        };
    assert_eq!(crlf, "title: Fictional\r\n");
}

#[test]
fn rejects_missing_front_matter_delimiters() {
    assert!(matches!(
        extract_front_matter("title: Fictional\n", &path(), 1024),
        Err(MarkdownValidationError::MissingOpeningDelimiter(_))
    ));
    assert!(matches!(
        extract_front_matter("---\ntitle: Fictional\n", &path(), 1024),
        Err(MarkdownValidationError::MissingClosingDelimiter(_))
    ));
}
