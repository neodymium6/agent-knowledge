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
      containerArchitecture = {
        x86_64-linux = "amd64";
        aarch64-linux = "arm64";
      };
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
              ./deploy/systemd/agent-knowledge-queue-ingress.socket
              (./deploy/systemd + "/agent-knowledge-queue-ingress@.service")
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
            install -Dm644 deploy/systemd/agent-knowledge-queue-ingress.socket \
              "$out/lib/systemd/system/agent-knowledge-queue-ingress.socket"
            install -Dm644 deploy/systemd/agent-knowledge-queue-ingress@.service \
              "$out/lib/systemd/system/agent-knowledge-queue-ingress@.service"
            substituteInPlace "$out/lib/systemd/system/agent-knowledge-queue-ingress@.service" \
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
      workerContainerImageFor =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = packageFor system;
          rootFilesystem = pkgs.runCommand "agent-knowledge-container-root" { } ''
            install -d "$out/etc" "$out/var/empty" "$out/var/lib/agent-knowledge"
            install -m444 ${./deploy/container/passwd} "$out/etc/passwd"
            install -m444 ${./deploy/container/group} "$out/etc/group"
          '';
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-worker";
          tag = projectVersion;
          contents = [
            package
            pkgs.cacert
            rootFilesystem
          ];
          config = {
            User = "agent-knowledge";
            WorkingDir = "/var/lib/agent-knowledge";
            Entrypoint = [
              "${package}/bin/agent-knowledge"
              "worker"
              "run"
            ];
            Env = [
              "HOME=/var/lib/agent-knowledge"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge Worker";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
            };
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
        worker-container-image = workerContainerImageFor system;
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

      checks = forLinuxSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = packageFor system;
          workerContainerImage = workerContainerImageFor system;
        in
        {
          package = package;
          container-image =
            pkgs.runCommand "check-agent-knowledge-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${workerContainerImage} \
                      ${containerArchitecture.${system}} \
                      ${package}/bin/agent-knowledge \
                      ${projectVersion} \
                      ${./deploy/container/passwd} \
                      ${./deploy/container/group} \
                      ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
                    touch "$out"
              '';
        }
      );

      formatter = forAllSystems (system: (import nixpkgs { inherit system; }).nixfmt-tree);
    };
}
