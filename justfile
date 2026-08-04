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
  if [ "$(uname -s)" = Linux ]; then just check-package; fi

# Run source, test, and flake-schema checks without building a package.
check-code:
  pre-commit run --all-files
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  cargo test --workspace --all-features --locked
  nix flake check --no-build --all-systems .

# Build and validate the production package for the current Linux system.
check-package:
  package_path="$(nix build .#agent-knowledge --no-link --print-out-paths)" && deploy/systemd/check-package.sh "$package_path"
  system="$(nix eval --impure --raw --expr builtins.currentSystem)" && nix build ".#checks.$system.worker-container-image" ".#checks.$system.queue-ingress-container-image" ".#checks.$system.gateway-container-image" ".#checks.$system.openssh-gateway-container-image" ".#checks.$system.storage-bootstrap-container-image" --no-link

# CI source-check alias; package jobs build each supported architecture.
ci: check-code check-kubernetes

# Render and validate the single-replica Kubernetes deployment.
check-kubernetes:
  nix develop . --command bash deploy/kubernetes/check-manifests.sh deploy/kubernetes
  nix develop . --command bash deploy/kubernetes-e2e/check.sh deploy/kubernetes-e2e

# Run the disposable kind cluster and full SSH persistence test.
test-kubernetes-e2e:
  nix develop . --command bash deploy/kubernetes-e2e/run.sh

# Update pinned development-environment inputs.
update:
  nix flake update
