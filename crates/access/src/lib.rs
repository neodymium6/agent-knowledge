//! SSH client enrollment records and durable access-registry storage.

mod authorized_keys;
mod key;
mod store;

pub use authorized_keys::{
    AuthorizedKeysImportError, AuthorizedKeysRenderError, ImportedClient, parse_authorized_keys,
    write_authorized_keys,
};
pub use key::{KeyValidationError, NormalizedPublicKey};
pub use store::{
    AccessRegistry, AccessRegistryError, ClientRecord, ClientStatus, MutationOutcome,
    RegistrySnapshot,
};
