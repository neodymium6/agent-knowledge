use std::sync::OnceLock;

use lindera::dictionary::load_dictionary;
use lindera::mode::Mode;
use lindera::segmenter::Segmenter;
use lindera_analysis::character_filter::unicode_normalize::{
    UnicodeNormalizeCharacterFilter, UnicodeNormalizeKind,
};
use lindera_analysis::token_filter::TokenFilter;
use lindera_tantivy::tokenizer::LinderaTokenizer;
use tantivy::tokenizer::{LowerCaser, TextAnalyzer};

use super::TantivySearchError;

// Change this identity when the dictionary or analysis pipeline changes, even
// when the Tantivy schema and canonical commit would otherwise stay identical.
pub(super) const ANALYZER: &str = "ja-ipadic-20250920-lindera5.0.1-dictionary5.3.0-nfkc-lower-v1";

pub(super) fn register(index: &tantivy::Index) -> Result<(), TantivySearchError> {
    static ANALYSIS: OnceLock<Result<TextAnalyzer, String>> = OnceLock::new();
    let analyzer = ANALYSIS.get_or_init(|| {
        let dictionary = load_dictionary("embedded://ipadic").map_err(|e| e.to_string())?;
        let mut tokenizer =
            LinderaTokenizer::from_segmenter(Segmenter::new(Mode::Normal, dictionary, None));
        tokenizer.append_character_filter(
            UnicodeNormalizeCharacterFilter::new(UnicodeNormalizeKind::NFKC).into(),
        );
        tokenizer.append_token_filter(SearchTerms.into());
        Ok(TextAnalyzer::builder(tokenizer).filter(LowerCaser).build())
    });
    let analyzer = analyzer.as_ref().map_err(|message| {
        TantivySearchError::engine(tantivy::TantivyError::InvalidArgument(message.clone()))
    })?;
    index.tokenizers().register(ANALYZER, analyzer.clone());
    Ok(())
}

// Ignore whitespace and punctuation without discarding Japanese or other
// non-ASCII words. Keep the original token positions and corrected offsets.
#[derive(Clone)]
struct SearchTerms;

impl TokenFilter for SearchTerms {
    fn name(&self) -> &'static str {
        "search_terms"
    }

    fn apply(&self, tokens: &mut Vec<lindera::token::Token<'_>>) -> lindera::LinderaResult<()> {
        tokens.retain(|token| token.surface.chars().any(char::is_alphanumeric));
        Ok(())
    }
}

/// Normalizes human-readable project discovery text, without changing exact IDs
/// or the Markdown returned to callers.
#[must_use]
pub fn normalize_search_text(text: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    text.nfkc().flat_map(char::to_lowercase).collect()
}
