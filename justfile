set shell := ["bash", "-euo", "pipefail", "-c"]

# Show available recipes.
default:
  @just --list

# Initialize Git when needed and install repository hooks.
init:
  if [ ! -d .git ]; then git init -b main; fi
  pre-commit install --install-hooks

# Format repository-owned files.
fmt:
  nix fmt -- flake.nix
  cargo fmt --all

# Run all repository checks.
check:
  pre-commit run --all-files
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  cargo test --workspace --all-features --locked
  nix flake check .

# CI alias.
ci: check

# Update pinned development-environment inputs.
update:
  nix flake update
