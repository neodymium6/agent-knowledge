//! Shared domain types and validation for Agent Knowledge.

/// The stable application name used by commands and diagnostics.
pub const APPLICATION_NAME: &str = "agent-knowledge";

#[cfg(test)]
mod tests {
    use super::APPLICATION_NAME;

    #[test]
    fn application_name_is_stable() {
        assert_eq!(APPLICATION_NAME, "agent-knowledge");
    }
}
