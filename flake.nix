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
      pkgsFor = forAllSystems (system: import nixpkgs { inherit system; });
      projectVersion = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      queueIngressService = builtins.path {
        path = ./deploy/systemd + "/agent-knowledge-queue-ingress@.service";
        name = "agent-knowledge-queue-ingress-instance.service";
      };
      containerArchitecture = {
        x86_64-linux = "amd64";
        aarch64-linux = "arm64";
      };
      unwrappedPackageForWith =
        {
          system,
          gitProgram ? null,
        }:
        let
          pkgs = pkgsFor.${system};
        in
        pkgs.rustPlatform.buildRustPackage (
          {
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
            doCheck = false;
            doInstallCheck = true;

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
          }
          // pkgs.lib.optionalAttrs (gitProgram != null) {
            AGENT_KNOWLEDGE_GIT_PROGRAM = gitProgram;
          }
        );
      unwrappedPackageFor = system: unwrappedPackageForWith { inherit system; };
      storageBootstrapUnwrappedPackageFor =
        system:
        unwrappedPackageForWith {
          inherit system;
          gitProgram = "${pkgsFor.${system}.gitMinimal}/bin/git";
        };
      packageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          unwrappedPackage = unwrappedPackageFor system;
        in
        pkgs.runCommand "agent-knowledge-${projectVersion}"
          {
            nativeBuildInputs = [ pkgs.makeWrapper ];
            pname = "agent-knowledge";
            version = projectVersion;
            meta = unwrappedPackage.meta;
          }
          ''
            mkdir -p "$out/bin"
            makeWrapper ${unwrappedPackage}/bin/agent-knowledge \
              "$out/bin/agent-knowledge" \
              --prefix PATH : ${
                pkgs.lib.makeBinPath [
                  pkgs.gitMinimal
                  pkgs.openssh
                ]
              }
            install -m755 ${unwrappedPackage}/bin/agent-knowledge-ssh-shell \
              "$out/bin/agent-knowledge-ssh-shell"

            install -Dm644 ${./deploy/systemd/agent-knowledge-worker.service} \
              "$out/lib/systemd/system/agent-knowledge-worker.service"
            substituteInPlace "$out/lib/systemd/system/agent-knowledge-worker.service" \
              --replace-fail '@agentKnowledge@' "$out"
            install -Dm644 ${./deploy/systemd/agent-knowledge-queue-ingress.socket} \
              "$out/lib/systemd/system/agent-knowledge-queue-ingress.socket"
            install -Dm644 ${queueIngressService} \
              "$out/lib/systemd/system/agent-knowledge-queue-ingress@.service"
            substituteInPlace "$out/lib/systemd/system/agent-knowledge-queue-ingress@.service" \
              --replace-fail '@agentKnowledge@' "$out"
            install -Dm644 ${./deploy/systemd/agent-knowledge.conf.sysusers} \
              "$out/lib/sysusers.d/agent-knowledge.conf"
            install -Dm644 ${./deploy/systemd/agent-knowledge.conf.tmpfiles} \
              "$out/lib/tmpfiles.d/agent-knowledge.conf"
          '';
      gatewayPackageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          unwrappedPackage = unwrappedPackageFor system;
        in
        pkgs.runCommand "agent-knowledge-gateway-${projectVersion}"
          {
            nativeBuildInputs = [ pkgs.makeWrapper ];
            pname = "agent-knowledge-gateway";
            version = projectVersion;
            meta = unwrappedPackage.meta;
          }
          ''
            mkdir -p "$out/bin"
            makeWrapper ${unwrappedPackage}/bin/agent-knowledge \
              "$out/bin/agent-knowledge" \
              --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.gitMinimal ]}
          '';
      opensshGatewayPackageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          unwrappedPackage = unwrappedPackageFor system;
        in
        pkgs.runCommand "agent-knowledge-openssh-gateway-${projectVersion}"
          {
            nativeBuildInputs = [ pkgs.makeBinaryWrapper ];
            pname = "agent-knowledge-openssh-gateway";
            version = projectVersion;
            meta = unwrappedPackage.meta;
          }
          ''
            mkdir -p "$out/bin"
            makeBinaryWrapper ${unwrappedPackage}/bin/agent-knowledge \
              "$out/bin/agent-knowledge" \
              --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.gitMinimal ]}
            install -m755 ${unwrappedPackage}/bin/agent-knowledge-ssh-shell \
              "$out/bin/agent-knowledge-ssh-shell"
          '';
      storageBootstrapPackageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          unwrappedPackage = storageBootstrapUnwrappedPackageFor system;
        in
        pkgs.runCommand "agent-knowledge-storage-bootstrap-${projectVersion}"
          {
            nativeBuildInputs = [ pkgs.makeWrapper ];
            pname = "agent-knowledge-storage-bootstrap";
            version = projectVersion;
            meta = unwrappedPackage.meta;
          }
          ''
            mkdir -p "$out/bin"
            makeWrapper ${unwrappedPackage}/bin/agent-knowledge \
              "$out/bin/agent-knowledge" \
              --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.gitMinimal ]}
          '';
      containerRootFilesystemFor =
        system:
        let
          pkgs = pkgsFor.${system};
        in
        pkgs.runCommand "agent-knowledge-container-root" { } ''
          install -d "$out/etc" "$out/var/empty" "$out/var/lib/agent-knowledge"
          install -m444 ${./deploy/container/passwd} "$out/etc/passwd"
          install -m444 ${./deploy/container/group} "$out/etc/group"
        '';
      workerContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = packageFor system;
          rootFilesystem = containerRootFilesystemFor system;
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
      queueIngressContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = unwrappedPackageFor system;
          rootFilesystem = containerRootFilesystemFor system;
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-queue-ingress";
          tag = projectVersion;
          contents = [
            package
            rootFilesystem
          ];
          config = {
            User = "agent-knowledge-queue";
            WorkingDir = "/var/lib/agent-knowledge";
            Entrypoint = [
              "${package}/bin/agent-knowledge"
              "queue-ingress"
              "listen"
            ];
            Env = [ "HOME=/var/lib/agent-knowledge" ];
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge Queue Ingress";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
            };
          };
        };
      gatewayContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = gatewayPackageFor system;
          rootFilesystem = containerRootFilesystemFor system;
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-gateway";
          tag = projectVersion;
          contents = [
            package
            rootFilesystem
          ];
          config = {
            User = "agent-knowledge-gateway";
            WorkingDir = "/var/empty";
            Entrypoint = [
              "${package}/bin/agent-knowledge"
              "gateway"
            ];
            Env = [ "HOME=/var/empty" ];
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge Gateway";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
            };
          };
        };
      opensshGatewayContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = opensshGatewayPackageFor system;
          rootFilesystem = pkgs.runCommand "agent-knowledge-openssh-gateway-root" { } ''
            install -d "$out/bin" "$out/etc" "$out/var/empty"
            install -m444 ${./deploy/container/openssh-gateway-passwd} \
              "$out/etc/passwd"
            install -m444 ${./deploy/container/openssh-gateway-group} \
              "$out/etc/group"
            ln -s ${package}/bin/agent-knowledge "$out/bin/agent-knowledge"
            ln -s ${package}/bin/agent-knowledge-ssh-shell \
              "$out/bin/agent-knowledge-ssh-shell"
            ln -s ${pkgs.openssh}/bin/sshd "$out/bin/sshd"
          '';
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-openssh-gateway";
          tag = projectVersion;
          contents = [
            package
            pkgs.openssh
            rootFilesystem
          ];
          config = {
            User = "0";
            WorkingDir = "/var/empty";
            Entrypoint = [
              "/bin/sshd"
              "-D"
              "-e"
              "-p"
              "2222"
              "-f"
              "/etc/agent-knowledge/sshd_config"
            ];
            Env = [ "HOME=/var/empty" ];
            ExposedPorts = {
              "2222/tcp" = { };
            };
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge OpenSSH Gateway";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
            };
          };
        };
      storageBootstrapContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = storageBootstrapPackageFor system;
          rootFilesystem = containerRootFilesystemFor system;
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-storage-bootstrap";
          tag = projectVersion;
          contents = [
            package
            rootFilesystem
          ];
          config = {
            User = "0";
            WorkingDir = "/var/empty";
            Entrypoint = [
              "${package}/bin/agent-knowledge"
              "admin"
              "bootstrap-storage"
            ];
            Env = [ "HOME=/var/empty" ];
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge Storage Bootstrap";
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
          pkgs = pkgsFor.${system};
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
                kustomize
                nixfmt-tree
                pre-commit
                rustc
                rustfmt
              ]
              ++ lib.optionals stdenv.isLinux [
                openssh
                systemd
              ];
          };
        }
      );

      packages = forLinuxSystems (system: rec {
        agent-knowledge = packageFor system;
        gateway-container-image = gatewayContainerImageFor system;
        openssh-gateway-package = opensshGatewayPackageFor system;
        openssh-gateway-container-image = opensshGatewayContainerImageFor system;
        storage-bootstrap-container-image = storageBootstrapContainerImageFor system;
        queue-ingress-container-image = queueIngressContainerImageFor system;
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
          pkgs = pkgsFor.${system};
          package = packageFor system;
          workerContainerImage = workerContainerImageFor system;
          workerContainerImageCheck =
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
                      agent-knowledge-worker \
                      agent-knowledge \
                      worker \
                      run \
                      /var/lib/agent-knowledge \
                      "Agent Knowledge Worker" \
                      ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
                    touch "$out"
              '';
          kubernetesManifestCheck =
            pkgs.runCommand "check-agent-knowledge-kubernetes-manifests"
              {
                nativeBuildInputs = [
                  pkgs.jq
                  pkgs.kube-linter
                  pkgs.kustomize
                  pkgs.yq-go
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/kubernetes/check-manifests.sh} \
                  ${./deploy/kubernetes}
                touch "$out"
              '';
        in
        {
          package = package;
          package-metadata =
            assert package.pname == "agent-knowledge";
            assert package.version == projectVersion;
            pkgs.runCommand "check-agent-knowledge-package-metadata" { } ''
              touch "$out"
            '';
          container-image = workerContainerImageCheck;
          kubernetes-manifests = kubernetesManifestCheck;
          worker-container-image = workerContainerImageCheck;
          queue-ingress-container-image =
            let
              queueIngressContainerImage = queueIngressContainerImageFor system;
              queueIngressPackage = unwrappedPackageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-queue-ingress-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${queueIngressContainerImage} \
                      ${containerArchitecture.${system}} \
                      ${queueIngressPackage}/bin/agent-knowledge \
                      ${projectVersion} \
                      ${./deploy/container/passwd} \
                      ${./deploy/container/group} \
                      agent-knowledge-queue-ingress \
                      agent-knowledge-queue \
                      queue-ingress \
                      listen \
                      /var/lib/agent-knowledge \
                      "Agent Knowledge Queue Ingress" \
                      -
                    touch "$out"
              '';
          gateway-container-image =
            let
              gatewayContainerImage = gatewayContainerImageFor system;
              gatewayPackage = gatewayPackageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-gateway-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${gatewayContainerImage} \
                      ${containerArchitecture.${system}} \
                      ${gatewayPackage}/bin/agent-knowledge \
                      ${projectVersion} \
                      ${./deploy/container/passwd} \
                      ${./deploy/container/group} \
                      agent-knowledge-gateway \
                      agent-knowledge-gateway \
                      gateway \
                      - \
                      /var/empty \
                      "Agent Knowledge Gateway" \
                      -
                    touch "$out"
              '';
          openssh-gateway-container-image =
            let
              opensshGatewayContainerImage = opensshGatewayContainerImageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-openssh-gateway-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${opensshGatewayContainerImage} \
                      ${containerArchitecture.${system}} \
                      /bin/sshd \
                      ${projectVersion} \
                      ${./deploy/container/openssh-gateway-passwd} \
                      ${./deploy/container/openssh-gateway-group} \
                      agent-knowledge-openssh-gateway \
                      0 \
                      openssh-gateway \
                      - \
                      /var/empty \
                      "Agent Knowledge OpenSSH Gateway" \
                      -
                    touch "$out"
              '';
          storage-bootstrap-container-image =
            let
              storageBootstrapContainerImage = storageBootstrapContainerImageFor system;
              storageBootstrapPackage = storageBootstrapPackageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-storage-bootstrap-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${storageBootstrapContainerImage} \
                      ${containerArchitecture.${system}} \
                      ${storageBootstrapPackage}/bin/agent-knowledge \
                      ${projectVersion} \
                      ${./deploy/container/passwd} \
                      ${./deploy/container/group} \
                      agent-knowledge-storage-bootstrap \
                      0 \
                      admin \
                      bootstrap-storage \
                      /var/empty \
                      "Agent Knowledge Storage Bootstrap" \
                      -
                    touch "$out"
              '';
        }
      );

      formatter = forAllSystems (system: pkgsFor.${system}.nixfmt-tree);
    };
}
