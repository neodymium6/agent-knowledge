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

# Run code checks and build any checks available for the current system.
check: check-code
  nix flake check .

# Run source, test, and flake-schema checks without building a package.
check-code:
  pre-commit run --all-files
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  cargo test --workspace --all-features --locked
  nix flake check --no-build --all-systems .

# Build and install-check the production package for the current system.
check-package:
  nix build .#agent-knowledge --no-link

# CI source-check alias; package jobs build each supported architecture.
ci: check-code

# Update pinned development-environment inputs.
update:
  nix flake update
