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
              ./deploy/systemd/agent-knowledge-worker.service
              ./deploy/systemd/agent-knowledge.conf.sysusers
              ./deploy/systemd/agent-knowledge.conf.tmpfiles
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

            install -Dm644 deploy/systemd/agent-knowledge-worker.service \
              "$out/lib/systemd/system/agent-knowledge-worker.service"
            substituteInPlace "$out/lib/systemd/system/agent-knowledge-worker.service" \
              --replace-fail '@agentKnowledge@' "$out"
            install -Dm644 deploy/systemd/agent-knowledge.conf.sysusers \
              "$out/lib/sysusers.d/agent-knowledge.conf"
            install -Dm644 deploy/systemd/agent-knowledge.conf.tmpfiles \
              "$out/lib/tmpfiles.d/agent-knowledge.conf"
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
            packages =
              with pkgs;
              [
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
              ]
              ++ lib.optionals stdenv.isLinux [ systemd ];
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
