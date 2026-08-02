{
  description = "Agent Knowledge package and development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      linuxSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      forLinuxSystems = nixpkgs.lib.genAttrs linuxSystems;
      projectVersion = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      packageFor =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "agent-knowledge";
          version = projectVersion;

          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.lock
              ./Cargo.toml
              ./crates
              ./src
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;

          cargoBuildFlags = [
            "--workspace"
            "--all-features"
          ];
          nativeBuildInputs = [ pkgs.makeWrapper ];
          doCheck = false;
          doInstallCheck = true;

          postInstall = ''
            wrapProgram "$out/bin/agent-knowledge" \
              --prefix PATH : ${
                pkgs.lib.makeBinPath [
                  pkgs.gitMinimal
                  pkgs.openssh
                ]
              }
          '';

          installCheckPhase = ''
            runHook preInstallCheck
            set +e
            "$out/bin/agent-knowledge" >command-output 2>&1
            status=$?
            set -e
            test "$status" -eq 2
            grep -F "usage:" command-output
            runHook postInstallCheck
          '';

          meta = {
            description = "Centralized file-based knowledge management for coding agents";
            mainProgram = "agent-knowledge";
            platforms = linuxSystems;
          };
        };
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              actionlint
              cargo
              clippy
              gh
              git
              just
              nixfmt-tree
              pre-commit
              rustc
              rustfmt
            ];
          };
        }
      );

      packages = forLinuxSystems (system: rec {
        agent-knowledge = packageFor system;
        default = agent-knowledge;
      });

      apps = forLinuxSystems (system: rec {
        agent-knowledge = {
          type = "app";
          program = "${packageFor system}/bin/agent-knowledge";
          meta.description = "Run Agent Knowledge";
        };
        default = agent-knowledge;
      });

      checks = forLinuxSystems (system: {
        package = packageFor system;
      });

      formatter = forAllSystems (system: (import nixpkgs { inherit system; }).nixfmt-tree);
    };
}
