use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_knowledge_core::{BoundedFileError, PinnedRegularFile};
use agent_knowledge_protocol::ClientId;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

use crate::{ImportedClient, NormalizedPublicKey};

const BY_ID_DIRECTORY: &str = "by-id";
const STAGING_DIRECTORY: &str = ".staging";
const CURRENT_ENTRY: &str = "current";
const REGISTRY_FILE: &str = "registry.json";
const CURRENT_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_REGISTRY_BYTES: u64 = 4 * 1024 * 1024;
const MAXIMUM_CLIENTS: usize = 4_096;
const MAXIMUM_ACTOR_BYTES: usize = 128;

/// Whether a registered client key is accepted by the SSH adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientStatus {
    /// The public key may authenticate as this client.
    Active,
    /// The record is retained but the public key must not authenticate.
    Disabled,
}

/// One durable SSH client enrollment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientRecord {
    client_id: ClientId,
    public_key: String,
    fingerprint: String,
    status: ClientStatus,
    created_at: String,
    updated_at: String,
}

impl ClientRecord {
    /// Returns the stable identity bound to this key.
    #[must_use]
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the canonical OpenSSH public key without a comment.
    #[must_use]
    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    /// Returns the SHA-256 OpenSSH key fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Returns whether this record currently permits authentication.
    #[must_use]
    pub const fn status(&self) -> ClientStatus {
        self.status
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChangeAction {
    Add,
    Disable,
    Enable,
    RotateKey,
    ImportAuthorizedKeys,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RegistryChange {
    action: ChangeAction,
    actor: String,
    client_ids: Vec<ClientId>,
}

/// One immutable, complete generation of the client access registry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrySnapshot {
    schema_version: u16,
    generation_id: String,
    previous_generation: Option<String>,
    changed_at: String,
    change: RegistryChange,
    clients: Vec<ClientRecord>,
}

impl RegistrySnapshot {
    /// Returns the immutable generation identifier.
    #[must_use]
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    /// Returns the preceding generation, if this is not the first mutation.
    #[must_use]
    pub fn previous_generation(&self) -> Option<&str> {
        self.previous_generation.as_deref()
    }

    /// Returns clients sorted by client ID.
    #[must_use]
    pub fn clients(&self) -> &[ClientRecord] {
        &self.clients
    }
}

/// Result of an idempotent registry mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationOutcome {
    changed: bool,
    snapshot: RegistrySnapshot,
}

impl MutationOutcome {
    /// Returns whether a new generation was activated.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.changed
    }

    /// Returns the active snapshot after the operation.
    #[must_use]
    pub fn snapshot(&self) -> &RegistrySnapshot {
        &self.snapshot
    }
}

/// Durable SSH client registry with immutable generations and atomic selection.
#[derive(Clone, Debug)]
pub struct AccessRegistry {
    configured_root: PathBuf,
    stable_root: PathBuf,
    root_handle: Arc<File>,
    trusted_owner_uid: u32,
    mutation_available: Arc<AtomicBool>,
}

impl AccessRegistry {
    /// Creates or opens a registry trusted to the process's effective UID.
    ///
    /// # Errors
    ///
    /// Returns an error when the root or fixed entries are unsafe or owned by
    /// another user, or when the selected generation is invalid.
    pub fn open_for_effective_user(root: impl AsRef<Path>) -> Result<Self, AccessRegistryError> {
        #[cfg(unix)]
        let trusted_owner_uid = nix::unistd::Uid::effective().as_raw();
        #[cfg(not(unix))]
        let trusted_owner_uid = 0;

        Self::open(root, trusted_owner_uid)
    }

    /// Creates or opens an access-registry layout below an existing parent.
    ///
    /// # Errors
    ///
    /// Returns an error when the root or fixed entries are unsafe or not owned
    /// by `trusted_owner_uid`, or when the selected generation is invalid.
    pub fn open(
        root: impl AsRef<Path>,
        trusted_owner_uid: u32,
    ) -> Result<Self, AccessRegistryError> {
        Self::open_with_initialization(root.as_ref(), trusted_owner_uid, true)
    }

    /// Opens an existing access-registry layout without creating storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the root or fixed entries do not exist, are
    /// unsafe, are not owned by the trusted UID, or select an invalid
    /// generation.
    pub fn open_existing(
        root: impl AsRef<Path>,
        trusted_owner_uid: u32,
    ) -> Result<Self, AccessRegistryError> {
        Self::open_with_initialization(root.as_ref(), trusted_owner_uid, false)
    }

    fn open_with_initialization(
        root: &Path,
        trusted_owner_uid: u32,
        initialize: bool,
    ) -> Result<Self, AccessRegistryError> {
        if initialize {
            ensure_directory(root)?;
        }
        let configured_root = fs::canonicalize(root).map_err(AccessRegistryError::Io)?;
        let root_handle = Arc::new(open_directory(&configured_root)?);
        let stable_root = stable_directory_path(&root_handle, &configured_root)?;
        validate_pinned_directory(&configured_root, &root_handle)?;
        let root_metadata = root_handle.metadata().map_err(AccessRegistryError::Io)?;
        validate_registry_directory(&root_metadata, &root_metadata, trusted_owner_uid)?;
        if initialize {
            ensure_directory(&stable_root.join(BY_ID_DIRECTORY))?;
            ensure_directory(&stable_root.join(STAGING_DIRECTORY))?;
            sync_directory(&stable_root)?;
        }
        let registry = Self {
            configured_root,
            stable_root,
            root_handle,
            trusted_owner_uid,
            mutation_available: Arc::new(AtomicBool::new(true)),
        };
        registry.validate_layout()?;
        let _ = registry.current()?;
        Ok(registry)
    }

    /// Reads the atomically selected generation without modifying storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the selector, immutable generation, or registry
    /// contents are invalid.
    pub fn current(&self) -> Result<Option<RegistrySnapshot>, AccessRegistryError> {
        self.validate_layout()?;
        let target = match fs::read_link(self.stable_root.join(CURRENT_ENTRY)) {
            Ok(target) => target,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(AccessRegistryError::Io(error)),
        };
        let generation_id = generation_from_target(&target)?;
        let generation = self.stable_root.join(BY_ID_DIRECTORY).join(&generation_id);
        let generation_handle = open_directory(&generation)?;
        validate_pinned_directory(&generation, &generation_handle)?;
        validate_registry_directory(
            &generation_handle
                .metadata()
                .map_err(AccessRegistryError::Io)?,
            &self
                .root_handle
                .metadata()
                .map_err(AccessRegistryError::Io)?,
            self.trusted_owner_uid,
        )?;
        let stable_generation = stable_directory_path(&generation_handle, &generation)?;
        let mut file = PinnedRegularFile::open_no_follow(stable_generation.join(REGISTRY_FILE))
            .map_err(AccessRegistryError::RegistryFile)?;
        validate_registry_file(
            &file.metadata().map_err(AccessRegistryError::Io)?,
            self.trusted_owner_uid,
        )?;
        if file.byte_length() > MAXIMUM_REGISTRY_BYTES {
            return Err(AccessRegistryError::RegistryTooLarge);
        }
        let mut bytes = Vec::with_capacity(file.byte_length() as usize);
        file.by_ref()
            .take(MAXIMUM_REGISTRY_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(AccessRegistryError::Io)?;
        if bytes.len() as u64 > MAXIMUM_REGISTRY_BYTES {
            return Err(AccessRegistryError::RegistryTooLarge);
        }
        let snapshot: RegistrySnapshot =
            serde_json::from_slice(&bytes).map_err(AccessRegistryError::Json)?;
        validate_snapshot(&snapshot, &generation_id)?;
        validate_pinned_directory(&generation, &generation_handle)?;
        self.validate_layout()?;
        Ok(Some(snapshot))
    }

    /// Registers a new client or returns the identical active record.
    pub fn add(
        &self,
        client_id: ClientId,
        public_key: NormalizedPublicKey,
        actor: &str,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        self.mutate(
            actor,
            ChangeAction::Add,
            vec![client_id.clone()],
            |clients, now| {
                if let Some(existing) = clients.iter().find(|client| client.client_id == client_id)
                {
                    return if existing.public_key == public_key.openssh()
                        && existing.status == ClientStatus::Active
                    {
                        Ok(false)
                    } else {
                        Err(AccessRegistryError::ClientAlreadyExists(client_id))
                    };
                }
                reject_duplicate_key(clients, public_key.fingerprint(), None)?;
                clients.push(ClientRecord {
                    client_id,
                    public_key: public_key.openssh().to_owned(),
                    fingerprint: public_key.fingerprint().to_owned(),
                    status: ClientStatus::Active,
                    created_at: now.to_owned(),
                    updated_at: now.to_owned(),
                });
                Ok(true)
            },
        )
    }

    /// Retains a client record while denying future authentication.
    pub fn disable(
        &self,
        client_id: &ClientId,
        actor: &str,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        self.set_status(
            client_id,
            ClientStatus::Disabled,
            actor,
            ChangeAction::Disable,
        )
    }

    /// Re-enables a retained client key.
    pub fn enable(
        &self,
        client_id: &ClientId,
        actor: &str,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        self.set_status(client_id, ClientStatus::Active, actor, ChangeAction::Enable)
    }

    /// Atomically replaces one client's key after checking its current
    /// fingerprint.
    pub fn rotate_key(
        &self,
        client_id: &ClientId,
        expected_fingerprint: &str,
        public_key: NormalizedPublicKey,
        actor: &str,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        self.mutate(
            actor,
            ChangeAction::RotateKey,
            vec![client_id.clone()],
            |clients, now| {
                let index = clients
                    .iter()
                    .position(|client| &client.client_id == client_id)
                    .ok_or_else(|| AccessRegistryError::UnknownClient(client_id.clone()))?;
                if clients[index].fingerprint != expected_fingerprint {
                    return Err(AccessRegistryError::FingerprintConflict {
                        expected: expected_fingerprint.to_owned(),
                        actual: clients[index].fingerprint.clone(),
                    });
                }
                if clients[index].public_key == public_key.openssh() {
                    return Ok(false);
                }
                reject_duplicate_key(clients, public_key.fingerprint(), Some(index))?;
                clients[index].public_key = public_key.openssh().to_owned();
                clients[index].fingerprint = public_key.fingerprint().to_owned();
                clients[index].updated_at = now.to_owned();
                Ok(true)
            },
        )
    }

    /// Atomically imports clients parsed from the previous static key file.
    pub fn import_authorized_keys(
        &self,
        imported: Vec<ImportedClient>,
        actor: &str,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        if imported.is_empty() {
            return Err(AccessRegistryError::EmptyImport);
        }
        let mut imported_ids = BTreeSet::new();
        let mut imported_fingerprints = BTreeSet::new();
        for client in &imported {
            if !imported_ids.insert(client.client_id.clone())
                || !imported_fingerprints.insert(client.public_key.fingerprint().to_owned())
            {
                return Err(AccessRegistryError::DuplicateImportEntry);
            }
        }
        let changed_ids = imported
            .iter()
            .map(|client| client.client_id.clone())
            .collect();
        self.mutate(
            actor,
            ChangeAction::ImportAuthorizedKeys,
            changed_ids,
            move |clients, now| {
                let mut changed = false;
                for imported in imported {
                    if let Some(existing) = clients
                        .iter()
                        .find(|client| client.client_id == imported.client_id)
                    {
                        if existing.public_key == imported.public_key.openssh()
                            && existing.status == ClientStatus::Active
                        {
                            continue;
                        }
                        return Err(AccessRegistryError::ClientAlreadyExists(imported.client_id));
                    }
                    reject_duplicate_key(clients, imported.public_key.fingerprint(), None)?;
                    clients.push(ClientRecord {
                        client_id: imported.client_id,
                        public_key: imported.public_key.openssh().to_owned(),
                        fingerprint: imported.public_key.fingerprint().to_owned(),
                        status: ClientStatus::Active,
                        created_at: now.to_owned(),
                        updated_at: now.to_owned(),
                    });
                    changed = true;
                }
                Ok(changed)
            },
        )
    }

    fn set_status(
        &self,
        client_id: &ClientId,
        status: ClientStatus,
        actor: &str,
        action: ChangeAction,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        self.mutate(actor, action, vec![client_id.clone()], |clients, now| {
            let client = clients
                .iter_mut()
                .find(|client| &client.client_id == client_id)
                .ok_or_else(|| AccessRegistryError::UnknownClient(client_id.clone()))?;
            if client.status == status {
                return Ok(false);
            }
            client.status = status;
            client.updated_at = now.to_owned();
            Ok(true)
        })
    }

    fn mutate(
        &self,
        actor: &str,
        action: ChangeAction,
        changed_ids: Vec<ClientId>,
        mutation: impl FnOnce(&mut Vec<ClientRecord>, &str) -> Result<bool, AccessRegistryError>,
    ) -> Result<MutationOutcome, AccessRegistryError> {
        validate_actor(actor)?;
        let _lease = self.lock_mutation()?;
        self.validate_layout()?;
        let current = self.current()?;
        let mut clients = current
            .as_ref()
            .map_or_else(Vec::new, |snapshot| snapshot.clients.clone());
        let observed_now = OffsetDateTime::now_utc();
        let previous_changed_at = current
            .as_ref()
            .and_then(|snapshot| OffsetDateTime::parse(&snapshot.changed_at, &Rfc3339).ok());
        let effective_now = previous_changed_at.map_or(observed_now, |previous| {
            std::cmp::max(observed_now, previous)
        });
        let now = effective_now
            .format(&Rfc3339)
            .map_err(AccessRegistryError::Time)?;
        if !mutation(&mut clients, &now)? {
            let snapshot = current.ok_or(AccessRegistryError::NoActiveGeneration)?;
            self.validate_layout()?;
            return Ok(MutationOutcome {
                changed: false,
                snapshot,
            });
        }
        if clients.len() > MAXIMUM_CLIENTS {
            return Err(AccessRegistryError::TooManyClients);
        }
        clients.sort_by(|left, right| left.client_id.cmp(&right.client_id));
        let generation_id = Ulid::generate().to_string();
        let snapshot = RegistrySnapshot {
            schema_version: CURRENT_SCHEMA_VERSION,
            generation_id: generation_id.clone(),
            previous_generation: current.map(|snapshot| snapshot.generation_id),
            changed_at: now,
            change: RegistryChange {
                action,
                actor: actor.to_owned(),
                client_ids: changed_ids,
            },
            clients,
        };
        validate_snapshot(&snapshot, &generation_id)?;
        self.persist(&snapshot)?;
        self.validate_layout()?;
        Ok(MutationOutcome {
            changed: true,
            snapshot,
        })
    }

    fn persist(&self, snapshot: &RegistrySnapshot) -> Result<(), AccessRegistryError> {
        let staging = self
            .stable_root
            .join(STAGING_DIRECTORY)
            .join(&snapshot.generation_id);
        let destination = self
            .stable_root
            .join(BY_ID_DIRECTORY)
            .join(&snapshot.generation_id);
        create_private_directory(&staging)?;
        let write_result = (|| {
            let root_metadata = self
                .root_handle
                .metadata()
                .map_err(AccessRegistryError::Io)?;
            let staging_metadata =
                fs::symlink_metadata(&staging).map_err(AccessRegistryError::Io)?;
            validate_registry_directory(&staging_metadata, &root_metadata, self.trusted_owner_uid)?;
            let mut bytes =
                serde_json::to_vec_pretty(snapshot).map_err(AccessRegistryError::Json)?;
            bytes.push(b'\n');
            if bytes.len() as u64 > MAXIMUM_REGISTRY_BYTES {
                return Err(AccessRegistryError::RegistryTooLarge);
            }
            let registry_file = staging.join(REGISTRY_FILE);
            write_new_file(&registry_file, &bytes)?;
            validate_registry_file(
                &fs::symlink_metadata(&registry_file).map_err(AccessRegistryError::Io)?,
                self.trusted_owner_uid,
            )?;
            sync_directory(&staging)?;
            fs::rename(&staging, &destination).map_err(AccessRegistryError::Io)?;
            sync_directory(&self.stable_root.join(BY_ID_DIRECTORY))?;
            sync_directory(&self.stable_root.join(STAGING_DIRECTORY))?;
            self.replace_current(&snapshot.generation_id)
        })();
        if write_result.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        write_result
    }

    fn replace_current(&self, generation_id: &str) -> Result<(), AccessRegistryError> {
        let target = PathBuf::from(BY_ID_DIRECTORY).join(generation_id);
        let temporary = self
            .stable_root
            .join(format!(".current-{}", Ulid::generate()));
        create_symlink(&target, &temporary)?;
        if let Err(error) = fs::rename(&temporary, self.stable_root.join(CURRENT_ENTRY)) {
            let _ = fs::remove_file(&temporary);
            return Err(AccessRegistryError::Io(error));
        }
        sync_directory(&self.stable_root)
    }

    fn validate_layout(&self) -> Result<(), AccessRegistryError> {
        validate_pinned_directory(&self.configured_root, &self.root_handle)?;
        let root_metadata = self
            .root_handle
            .metadata()
            .map_err(AccessRegistryError::Io)?;
        validate_registry_directory(&root_metadata, &root_metadata, self.trusted_owner_uid)?;
        for entry in [BY_ID_DIRECTORY, STAGING_DIRECTORY] {
            let metadata = fs::symlink_metadata(self.stable_root.join(entry))
                .map_err(AccessRegistryError::Io)?;
            if !metadata.file_type().is_dir() {
                return Err(AccessRegistryError::InvalidStorage);
            }
            validate_registry_directory(&metadata, &root_metadata, self.trusted_owner_uid)?;
        }
        match fs::symlink_metadata(self.stable_root.join(CURRENT_ENTRY)) {
            Ok(metadata) if metadata.file_type().is_symlink() => Ok(()),
            Ok(_) => Err(AccessRegistryError::InvalidCurrentEntry),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(AccessRegistryError::Io(error)),
        }
    }

    fn lock_mutation(&self) -> Result<MutationLease<'_>, AccessRegistryError> {
        if self
            .mutation_available
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(AccessRegistryError::Busy);
        }
        let root_lock = match self.root_handle.try_clone() {
            Ok(root) => root,
            Err(error) => {
                self.mutation_available.store(true, Ordering::Release);
                return Err(AccessRegistryError::Io(error));
            }
        };
        match root_lock.try_lock() {
            Ok(()) => Ok(MutationLease {
                available: &self.mutation_available,
                root_lock,
            }),
            Err(TryLockError::WouldBlock) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(AccessRegistryError::Busy)
            }
            Err(TryLockError::Error(error)) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(AccessRegistryError::Io(error))
            }
        }
    }
}

struct MutationLease<'a> {
    available: &'a AtomicBool,
    root_lock: File,
}

impl Drop for MutationLease<'_> {
    fn drop(&mut self) {
        let _ = self.root_lock.unlock();
        self.available.store(true, Ordering::Release);
    }
}

fn validate_snapshot(
    snapshot: &RegistrySnapshot,
    generation_id: &str,
) -> Result<(), AccessRegistryError> {
    if snapshot.schema_version != CURRENT_SCHEMA_VERSION
        || snapshot.generation_id != generation_id
        || Ulid::from_string(generation_id).is_err()
        || snapshot.clients.len() > MAXIMUM_CLIENTS
        || snapshot.change.client_ids.is_empty()
        || snapshot.change.client_ids.len() > MAXIMUM_CLIENTS
        || snapshot.change.actor.is_empty()
        || snapshot.change.actor.len() > MAXIMUM_ACTOR_BYTES
    {
        return Err(AccessRegistryError::InvalidSnapshot);
    }
    if let Some(previous) = &snapshot.previous_generation
        && (previous == generation_id || Ulid::from_string(previous).is_err())
    {
        return Err(AccessRegistryError::InvalidSnapshot);
    }
    let changed_at = OffsetDateTime::parse(&snapshot.changed_at, &Rfc3339)
        .map_err(|_| AccessRegistryError::InvalidSnapshot)?;
    let changed_client_ids = snapshot.change.client_ids.iter().collect::<BTreeSet<_>>();
    if changed_client_ids.len() != snapshot.change.client_ids.len() {
        return Err(AccessRegistryError::InvalidSnapshot);
    }
    let mut client_ids = BTreeSet::new();
    let mut fingerprints = BTreeSet::new();
    let mut previous_id: Option<&ClientId> = None;
    for client in &snapshot.clients {
        if previous_id.is_some_and(|previous| previous >= &client.client_id)
            || !client_ids.insert(client.client_id.clone())
            || !fingerprints.insert(client.fingerprint.clone())
        {
            return Err(AccessRegistryError::InvalidSnapshot);
        }
        previous_id = Some(&client.client_id);
        let key = NormalizedPublicKey::parse(&client.public_key)
            .map_err(|_| AccessRegistryError::InvalidSnapshot)?;
        if key.openssh() != client.public_key || key.fingerprint() != client.fingerprint {
            return Err(AccessRegistryError::InvalidSnapshot);
        }
        let created_at = OffsetDateTime::parse(&client.created_at, &Rfc3339)
            .map_err(|_| AccessRegistryError::InvalidSnapshot)?;
        let updated_at = OffsetDateTime::parse(&client.updated_at, &Rfc3339)
            .map_err(|_| AccessRegistryError::InvalidSnapshot)?;
        if created_at > updated_at || updated_at > changed_at {
            return Err(AccessRegistryError::InvalidSnapshot);
        }
    }
    if snapshot
        .change
        .client_ids
        .iter()
        .any(|client_id| !client_ids.contains(client_id))
    {
        return Err(AccessRegistryError::InvalidSnapshot);
    }
    Ok(())
}

fn validate_actor(actor: &str) -> Result<(), AccessRegistryError> {
    if actor.is_empty()
        || actor.len() > MAXIMUM_ACTOR_BYTES
        || !actor
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
    {
        Err(AccessRegistryError::InvalidActor)
    } else {
        Ok(())
    }
}

fn reject_duplicate_key(
    clients: &[ClientRecord],
    fingerprint: &str,
    except: Option<usize>,
) -> Result<(), AccessRegistryError> {
    if let Some((_, client)) = clients
        .iter()
        .enumerate()
        .find(|(index, client)| Some(*index) != except && client.fingerprint == fingerprint)
    {
        Err(AccessRegistryError::KeyAlreadyRegistered(
            client.client_id.clone(),
        ))
    } else {
        Ok(())
    }
}

fn generation_from_target(target: &Path) -> Result<String, AccessRegistryError> {
    let mut components = target.components();
    let by_id = components.next();
    let generation = components.next();
    if by_id != Some(Component::Normal(BY_ID_DIRECTORY.as_ref())) || components.next().is_some() {
        return Err(AccessRegistryError::InvalidCurrentEntry);
    }
    let generation = generation
        .and_then(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .ok_or(AccessRegistryError::InvalidCurrentEntry)?;
    Ulid::from_string(generation).map_err(|_| AccessRegistryError::InvalidCurrentEntry)?;
    Ok(generation.to_owned())
}

fn ensure_directory(path: &Path) -> Result<(), AccessRegistryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(AccessRegistryError::InvalidStorage),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o750);
                builder.create(path).map_err(AccessRegistryError::Io)?;
                set_exact_directory_mode(&open_directory(path)?)?;
            }
            #[cfg(not(unix))]
            {
                fs::create_dir(path).map_err(AccessRegistryError::Io)?;
            }
            sync_directory(path)?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            sync_directory(parent)
        }
        Err(error) => Err(AccessRegistryError::Io(error)),
    }
}

fn create_private_directory(path: &Path) -> Result<(), AccessRegistryError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o750);
        builder.create(path).map_err(AccessRegistryError::Io)?;
        set_exact_directory_mode(&open_directory(path)?)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path).map_err(AccessRegistryError::Io)
    }
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), AccessRegistryError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o640)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(AccessRegistryError::Io)?;
    #[cfg(unix)]
    set_exact_mode(&file, 0o640)?;
    file.write_all(bytes).map_err(AccessRegistryError::Io)?;
    file.sync_all().map_err(AccessRegistryError::Io)
}

#[cfg(unix)]
fn set_exact_mode(file: &File, mode: u32) -> Result<(), AccessRegistryError> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(mode))
        .map_err(AccessRegistryError::Io)
}

#[cfg(unix)]
fn set_exact_directory_mode(file: &File) -> Result<(), AccessRegistryError> {
    use std::os::unix::fs::PermissionsExt;

    let inherited_set_group_id = file
        .metadata()
        .map_err(AccessRegistryError::Io)?
        .permissions()
        .mode()
        & 0o2000;
    set_exact_mode(file, 0o750 | inherited_set_group_id)
}

#[cfg(unix)]
fn open_directory(path: &Path) -> Result<File, AccessRegistryError> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(AccessRegistryError::Io)
}

#[cfg(not(unix))]
fn open_directory(path: &Path) -> Result<File, AccessRegistryError> {
    File::open(path).map_err(AccessRegistryError::Io)
}

fn stable_directory_path(handle: &File, configured: &Path) -> Result<PathBuf, AccessRegistryError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let _ = configured;
        let stable = PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            handle.as_raw_fd()
        ));
        if !fs::metadata(&stable)
            .map_err(AccessRegistryError::Io)?
            .is_dir()
        {
            return Err(AccessRegistryError::InvalidStorage);
        }
        Ok(stable)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = handle;
        Ok(configured.to_owned())
    }
}

fn validate_pinned_directory(path: &Path, pinned: &File) -> Result<(), AccessRegistryError> {
    let live = fs::symlink_metadata(path).map_err(AccessRegistryError::Io)?;
    let pinned = pinned.metadata().map_err(AccessRegistryError::Io)?;
    if !live.file_type().is_dir() || !same_file(&live, &pinned) {
        Err(AccessRegistryError::StorageBindingChanged)
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn validate_registry_directory(
    metadata: &fs::Metadata,
    root_metadata: &fs::Metadata,
    trusted_owner_uid: u32,
) -> Result<(), AccessRegistryError> {
    use std::os::unix::fs::MetadataExt;
    if metadata.uid() != trusted_owner_uid {
        return Err(AccessRegistryError::UnexpectedOwner {
            expected_uid: trusted_owner_uid,
            actual_uid: metadata.uid(),
        });
    }
    if metadata.mode() & 0o022 != 0 {
        return Err(AccessRegistryError::InsecurePermissions);
    }
    if metadata.dev() != root_metadata.dev() {
        return Err(AccessRegistryError::CrossFilesystemStorage);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_registry_directory(
    _metadata: &fs::Metadata,
    _root_metadata: &fs::Metadata,
    _trusted_owner_uid: u32,
) -> Result<(), AccessRegistryError> {
    Ok(())
}

#[cfg(unix)]
fn validate_registry_file(
    metadata: &fs::Metadata,
    trusted_owner_uid: u32,
) -> Result<(), AccessRegistryError> {
    use std::os::unix::fs::MetadataExt;
    if metadata.uid() != trusted_owner_uid {
        return Err(AccessRegistryError::UnexpectedOwner {
            expected_uid: trusted_owner_uid,
            actual_uid: metadata.uid(),
        });
    }
    if metadata.mode() & 0o022 != 0 {
        return Err(AccessRegistryError::InsecurePermissions);
    }
    if metadata.nlink() != 1 {
        return Err(AccessRegistryError::HardLinkedRegistryFile);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_registry_file(
    _metadata: &fs::Metadata,
    _trusted_owner_uid: u32,
) -> Result<(), AccessRegistryError> {
    Ok(())
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type() && left.len() == right.len()
}

fn sync_directory(path: &Path) -> Result<(), AccessRegistryError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(AccessRegistryError::Io)
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<(), AccessRegistryError> {
    std::os::unix::fs::symlink(target, link).map_err(AccessRegistryError::Io)
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, _link: &Path) -> Result<(), AccessRegistryError> {
    Err(AccessRegistryError::UnsupportedPlatform)
}

/// Failure to read or atomically mutate the access registry.
#[derive(Debug)]
pub enum AccessRegistryError {
    /// Filesystem I/O failed.
    Io(io::Error),
    /// Registry JSON could not be encoded or decoded.
    Json(serde_json::Error),
    /// A registry file could not be safely pinned.
    RegistryFile(BoundedFileError),
    /// A timestamp could not be formatted.
    Time(time::error::Format),
    /// Another writer currently owns the registry.
    Busy,
    /// The configured root or a fixed entry was not a real directory.
    InvalidStorage,
    /// The root path stopped naming the opened directory.
    StorageBindingChanged,
    /// Registry storage was writable by an unrelated group or user.
    InsecurePermissions,
    /// Registry storage was not owned by the configured administrative UID.
    UnexpectedOwner { expected_uid: u32, actual_uid: u32 },
    /// Fixed registry directories did not share one filesystem.
    CrossFilesystemStorage,
    /// An immutable registry file had another hard link.
    HardLinkedRegistryFile,
    /// The active selector was not the exact relative generation shape.
    InvalidCurrentEntry,
    /// The selected immutable snapshot failed validation.
    InvalidSnapshot,
    /// Registry JSON exceeded the fixed bound.
    RegistryTooLarge,
    /// The configured client limit was reached.
    TooManyClients,
    /// No active generation exists for an idempotent no-op.
    NoActiveGeneration,
    /// The audit actor label was malformed.
    InvalidActor,
    /// A client ID was already bound to different state.
    ClientAlreadyExists(ClientId),
    /// A requested client does not exist.
    UnknownClient(ClientId),
    /// A public key was already assigned to another client.
    KeyAlreadyRegistered(ClientId),
    /// Key rotation observed a different current fingerprint.
    FingerprintConflict { expected: String, actual: String },
    /// An import did not contain any client records.
    EmptyImport,
    /// An import repeated a client ID or public key.
    DuplicateImportEntry,
    /// Atomic symlink selection is unavailable.
    UnsupportedPlatform,
}

impl fmt::Display for AccessRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "access registry I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "access registry JSON failed: {error}"),
            Self::RegistryFile(error) => write!(formatter, "invalid registry file: {error}"),
            Self::Time(error) => write!(formatter, "could not format registry timestamp: {error}"),
            Self::Busy => formatter.write_str("another access registry writer is active"),
            Self::InvalidStorage => formatter.write_str("invalid access registry storage layout"),
            Self::StorageBindingChanged => {
                formatter.write_str("access registry storage binding changed")
            }
            Self::InsecurePermissions => {
                formatter.write_str("access registry storage has insecure write permissions")
            }
            Self::UnexpectedOwner {
                expected_uid,
                actual_uid,
            } => write!(
                formatter,
                "access registry storage owner is UID {actual_uid}, expected UID {expected_uid}"
            ),
            Self::CrossFilesystemStorage => {
                formatter.write_str("access registry directories must share one filesystem")
            }
            Self::HardLinkedRegistryFile => {
                formatter.write_str("access registry snapshot must not be hard linked")
            }
            Self::InvalidCurrentEntry => formatter.write_str("invalid access registry selector"),
            Self::InvalidSnapshot => formatter.write_str("invalid access registry snapshot"),
            Self::RegistryTooLarge => formatter.write_str("access registry exceeds its size limit"),
            Self::TooManyClients => formatter.write_str("access registry client limit reached"),
            Self::NoActiveGeneration => formatter.write_str("access registry has no generation"),
            Self::InvalidActor => formatter.write_str("invalid access registry actor label"),
            Self::ClientAlreadyExists(client_id) => {
                write!(
                    formatter,
                    "client {client_id} already has different registry state"
                )
            }
            Self::UnknownClient(client_id) => write!(formatter, "unknown client: {client_id}"),
            Self::KeyAlreadyRegistered(client_id) => {
                write!(
                    formatter,
                    "SSH public key already belongs to client {client_id}"
                )
            }
            Self::FingerprintConflict { expected, actual } => write!(
                formatter,
                "SSH key fingerprint conflict: expected {expected}, current {actual}"
            ),
            Self::EmptyImport => formatter.write_str("authorized_keys import contains no clients"),
            Self::DuplicateImportEntry => {
                formatter.write_str("authorized_keys import contains a duplicate client or key")
            }
            Self::UnsupportedPlatform => {
                formatter.write_str("access registry requires atomic symbolic links")
            }
        }
    }
}

impl std::error::Error for AccessRegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::RegistryFile(error) => Some(error),
            Self::Time(error) => Some(error),
            Self::Busy
            | Self::InvalidStorage
            | Self::StorageBindingChanged
            | Self::InsecurePermissions
            | Self::UnexpectedOwner { .. }
            | Self::CrossFilesystemStorage
            | Self::HardLinkedRegistryFile
            | Self::InvalidCurrentEntry
            | Self::InvalidSnapshot
            | Self::RegistryTooLarge
            | Self::TooManyClients
            | Self::NoActiveGeneration
            | Self::InvalidActor
            | Self::ClientAlreadyExists(_)
            | Self::UnknownClient(_)
            | Self::KeyAlreadyRegistered(_)
            | Self::FingerprintConflict { .. }
            | Self::EmptyImport
            | Self::DuplicateImportEntry
            | Self::UnsupportedPlatform => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use agent_knowledge_protocol::ClientId;

    use super::{AccessRegistry, AccessRegistryError, ClientStatus, STAGING_DIRECTORY};
    use crate::{ImportedClient, NormalizedPublicKey};

    const KEY_A: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN796jTiQfZfG1KaT0PtFDJ/XFSqti fictional-a@example.invalid";
    const KEY_B: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKWz8j8C3gyf7u8sVvD6cGx0iW9F8uQ5yT6u7V8wX9yZ fictional-b@example.invalid";
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "agent-knowledge-access-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path)
                .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("test directory must be removed: {error}");
            }
        }
    }

    fn client_id(value: &str) -> ClientId {
        ClientId::from_str(value)
            .unwrap_or_else(|error| panic!("fixture client ID must be valid: {error}"))
    }

    fn key(value: &str) -> NormalizedPublicKey {
        NormalizedPublicKey::parse(value)
            .unwrap_or_else(|error| panic!("fixture key must be valid: {error}"))
    }

    fn trusted_owner_uid(path: &Path) -> Result<u32, AccessRegistryError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            let owner_path = if path.exists() {
                path
            } else {
                path.parent().unwrap_or(path)
            };
            Ok(fs::metadata(owner_path)
                .map_err(AccessRegistryError::Io)?
                .uid())
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(0)
        }
    }

    fn open_registry(path: impl AsRef<Path>) -> Result<AccessRegistry, AccessRegistryError> {
        let path = path.as_ref();
        AccessRegistry::open(path, trusted_owner_uid(path)?)
    }

    #[cfg(unix)]
    #[test]
    fn creates_an_exact_group_readable_layout() {
        use std::os::unix::fs::PermissionsExt;

        let root = TestDirectory::create();
        let registry_root = root.path().join("registry");
        fs::create_dir(&registry_root)
            .unwrap_or_else(|error| panic!("registry root must be created: {error}"));
        fs::set_permissions(&registry_root, fs::Permissions::from_mode(0o2750))
            .unwrap_or_else(|error| panic!("registry root mode must be set: {error}"));
        let registry = open_registry(&registry_root)
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        let outcome = registry
            .add(client_id("fictional-node-a"), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("client must be added: {error}"));
        let generation = registry_root
            .join("by-id")
            .join(outcome.snapshot().generation_id());

        for directory in [
            registry_root.clone(),
            registry_root.join("by-id"),
            registry_root.join(STAGING_DIRECTORY),
            generation.clone(),
        ] {
            let mode = fs::metadata(&directory)
                .unwrap_or_else(|error| panic!("directory mode must be readable: {error}"))
                .permissions()
                .mode()
                & 0o7777;
            assert_eq!(mode, 0o2750, "unexpected mode for {}", directory.display());
        }
        let file_mode = fs::metadata(generation.join("registry.json"))
            .unwrap_or_else(|error| panic!("registry mode must be readable: {error}"))
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o640);
    }

    #[test]
    fn adds_disables_enables_and_rotates_without_deleting_history() {
        let root = TestDirectory::create();
        let registry = open_registry(root.path().join("registry"))
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        let id = client_id("fictional-node-a");
        let added = registry
            .add(id.clone(), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("client must be added: {error}"));
        assert!(added.changed());
        let first_generation = added.snapshot().generation_id().to_owned();
        let disabled = registry
            .disable(&id, "local-admin")
            .unwrap_or_else(|error| panic!("client must be disabled: {error}"));
        assert_eq!(
            disabled.snapshot().clients()[0].status(),
            ClientStatus::Disabled
        );
        let enabled = registry
            .enable(&id, "local-admin")
            .unwrap_or_else(|error| panic!("client must be enabled: {error}"));
        let fingerprint = enabled.snapshot().clients()[0].fingerprint().to_owned();
        let rotated = registry
            .rotate_key(&id, &fingerprint, key(KEY_B), "local-admin")
            .unwrap_or_else(|error| panic!("key must rotate: {error}"));
        assert_eq!(
            rotated.snapshot().clients()[0].public_key(),
            key(KEY_B).openssh()
        );
        assert!(
            root.path()
                .join("registry/by-id")
                .join(first_generation)
                .join("registry.json")
                .is_file()
        );
    }

    #[test]
    fn treats_identical_add_and_status_changes_as_idempotent() {
        let root = TestDirectory::create();
        let registry = open_registry(root.path().join("registry"))
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        let id = client_id("fictional-node-a");
        let added = registry
            .add(id.clone(), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("client must be added: {error}"));
        let retried = registry
            .add(id.clone(), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("identical add must be idempotent: {error}"));
        assert!(!retried.changed());
        assert_eq!(
            retried.snapshot().generation_id(),
            added.snapshot().generation_id()
        );
        registry
            .disable(&id, "local-admin")
            .unwrap_or_else(|error| panic!("disable must succeed: {error}"));
        let retried = registry
            .disable(&id, "local-admin")
            .unwrap_or_else(|error| panic!("disable retry must succeed: {error}"));
        assert!(!retried.changed());
    }

    #[test]
    fn rejects_duplicate_keys_and_stale_rotation() {
        let root = TestDirectory::create();
        let registry = open_registry(root.path().join("registry"))
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        let id = client_id("fictional-node-a");
        registry
            .add(id.clone(), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("client must be added: {error}"));
        assert!(matches!(
            registry.add(client_id("fictional-node-b"), key(KEY_A), "local-admin"),
            Err(AccessRegistryError::KeyAlreadyRegistered(_))
        ));
        assert!(matches!(
            registry.rotate_key(&id, "SHA256:stale", key(KEY_B), "local-admin"),
            Err(AccessRegistryError::FingerprintConflict { .. })
        ));
    }

    #[test]
    fn imports_multiple_clients_in_one_generation() {
        let root = TestDirectory::create();
        let registry = open_registry(root.path().join("registry"))
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        let outcome = registry
            .import_authorized_keys(
                vec![
                    ImportedClient {
                        client_id: client_id("fictional-node-b"),
                        public_key: key(KEY_B),
                    },
                    ImportedClient {
                        client_id: client_id("fictional-node-a"),
                        public_key: key(KEY_A),
                    },
                ],
                "local-admin",
            )
            .unwrap_or_else(|error| panic!("import must succeed: {error}"));
        assert_eq!(outcome.snapshot().clients().len(), 2);
        assert_eq!(
            outcome.snapshot().clients()[0].client_id().as_str(),
            "fictional-node-a"
        );
    }

    #[test]
    fn rejects_duplicate_entries_within_one_import() {
        let root = TestDirectory::create();
        let registry = open_registry(root.path().join("registry"))
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        assert!(matches!(
            registry.import_authorized_keys(
                vec![
                    ImportedClient {
                        client_id: client_id("fictional-node-a"),
                        public_key: key(KEY_A),
                    },
                    ImportedClient {
                        client_id: client_id("fictional-node-a"),
                        public_key: key(KEY_A),
                    },
                ],
                "local-admin",
            ),
            Err(AccessRegistryError::DuplicateImportEntry)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_non_symlink_current_entry() {
        let root = TestDirectory::create();
        let registry_root = root.path().join("registry");
        let registry = open_registry(&registry_root)
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        drop(registry);
        fs::write(registry_root.join("current"), b"not a selector")
            .unwrap_or_else(|error| panic!("invalid selector fixture must be written: {error}"));
        assert!(matches!(
            open_registry(&registry_root),
            Err(AccessRegistryError::InvalidCurrentEntry)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_group_writable_storage_and_hard_linked_snapshots() {
        use std::os::unix::fs::PermissionsExt;

        let root = TestDirectory::create();
        let insecure_root = root.path().join("insecure");
        fs::create_dir(&insecure_root)
            .unwrap_or_else(|error| panic!("insecure fixture must be created: {error}"));
        fs::set_permissions(&insecure_root, fs::Permissions::from_mode(0o770))
            .unwrap_or_else(|error| panic!("fixture permissions must be changed: {error}"));
        assert!(matches!(
            open_registry(&insecure_root),
            Err(AccessRegistryError::InsecurePermissions)
        ));

        let registry_root = root.path().join("registry");
        let registry = open_registry(&registry_root)
            .unwrap_or_else(|error| panic!("registry must open: {error}"));
        registry
            .add(client_id("fictional-node-a"), key(KEY_A), "local-admin")
            .unwrap_or_else(|error| panic!("client must be added: {error}"));
        let generation = fs::read_link(registry_root.join("current"))
            .unwrap_or_else(|error| panic!("current selector must be readable: {error}"));
        let snapshot = registry_root.join(generation).join("registry.json");
        fs::hard_link(&snapshot, root.path().join("linked-registry.json"))
            .unwrap_or_else(|error| panic!("hard-link fixture must be created: {error}"));
        assert!(matches!(
            registry.current(),
            Err(AccessRegistryError::HardLinkedRegistryFile)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_storage_owned_by_a_different_uid() {
        let root = TestDirectory::create();
        let registry_root = root.path().join("registry");
        let actual_uid = trusted_owner_uid(&registry_root)
            .unwrap_or_else(|error| panic!("fixture owner must be readable: {error}"));
        let foreign_uid = actual_uid.wrapping_add(1);

        assert!(matches!(
            AccessRegistry::open(&registry_root, foreign_uid),
            Err(AccessRegistryError::UnexpectedOwner {
                expected_uid,
                actual_uid: observed_uid,
            }) if expected_uid == foreign_uid && observed_uid == actual_uid
        ));
    }

    #[test]
    fn does_not_create_missing_registry_parents() {
        let root = TestDirectory::create();
        let missing_parent = root.path().join("missing");
        let registry_root = missing_parent.join("registry");
        let owner_uid = trusted_owner_uid(root.path())
            .unwrap_or_else(|error| panic!("fixture owner must be readable: {error}"));

        assert!(matches!(
            AccessRegistry::open(&registry_root, owner_uid),
            Err(AccessRegistryError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound
        ));
        assert!(!missing_parent.exists());
    }

    #[test]
    fn opens_existing_storage_without_initializing_missing_entries() {
        let root = TestDirectory::create();
        let registry_root = root.path().join("registry");
        let registry = open_registry(&registry_root)
            .unwrap_or_else(|error| panic!("registry must initialize: {error}"));
        let owner_uid = trusted_owner_uid(&registry_root)
            .unwrap_or_else(|error| panic!("fixture owner must be readable: {error}"));
        AccessRegistry::open_existing(&registry_root, owner_uid)
            .unwrap_or_else(|error| panic!("existing registry must open: {error}"));

        fs::remove_dir(registry.stable_root.join(STAGING_DIRECTORY))
            .unwrap_or_else(|error| panic!("staging fixture must be removed: {error}"));
        assert!(AccessRegistry::open_existing(&registry_root, owner_uid).is_err());
        assert!(!registry_root.join(STAGING_DIRECTORY).exists());
    }
}
