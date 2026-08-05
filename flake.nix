{
  description = "Agent Knowledge package and development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    csi-driver-host-path = {
      url = "github:kubernetes-csi/csi-driver-host-path/cc78ee78ae23908c9e0607df2fe09c7ecfa52597";
      flake = false;
    };
    external-snapshotter = {
      url = "github:kubernetes-csi/external-snapshotter/78e32cd84e0abec2621924a30e38c755f93e180a";
      flake = false;
    };
  };

  outputs =
    {
      csi-driver-host-path,
      external-snapshotter,
      nixpkgs,
      ...
    }:
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
      projectLicense = "Apache-2.0";
      queueIngressService = builtins.path {
        path = ./deploy/systemd + "/agent-knowledge-queue-ingress@.service";
        name = "agent-knowledge-queue-ingress-instance.service";
      };
      containerArchitecture = {
        x86_64-linux = "amd64";
        aarch64-linux = "arm64";
      };
      clientRustTarget = {
        x86_64-linux = "x86_64-unknown-linux-musl";
        aarch64-linux = "aarch64-unknown-linux-musl";
      };
      staticClientPackageFor =
        system:
        let
          pkgs = pkgsFor.${system};
        in
        pkgs.pkgsStatic.rustPlatform.buildRustPackage {
          pname = "agent-knowledge-client";
          version = projectVersion;

          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.lock
              ./Cargo.toml
              ./crates
              ./README.md
              ./src
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          cargoBuildFlags = [
            "--package"
            "agent-knowledge-client"
            "--bin"
            "agent-knowledge-client"
          ];
          cargoTestFlags = [
            "--package"
            "agent-knowledge-client"
          ];
          doInstallCheck = true;
          installCheckPhase = ''
            runHook preInstallCheck
            test "$("$out/bin/agent-knowledge-client" --version)" = \
              "agent-knowledge-client ${projectVersion}"
            runHook postInstallCheck
          '';

          meta = {
            description = "Portable SSH client for Agent Knowledge";
            license = pkgs.lib.licenses.asl20;
            mainProgram = "agent-knowledge-client";
            platforms = linuxSystems;
          };
        };
      clientMcpPackageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          client = staticClientPackageFor system;
        in
        pkgs.runCommand "agent-knowledge-client-mcp-${projectVersion}"
          {
            nativeBuildInputs = [ pkgs.makeBinaryWrapper ];
            pname = "agent-knowledge-client-mcp";
            version = projectVersion;
            meta = client.meta;
          }
          ''
            mkdir -p "$out/bin"
            makeBinaryWrapper ${client}/bin/agent-knowledge-client \
              "$out/bin/agent-knowledge-client" \
              --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.openssh ]}
          '';
      clientReleaseArchiveFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = staticClientPackageFor system;
          archiveRoot = "agent-knowledge-client-v${projectVersion}-${clientRustTarget.${system}}";
        in
        pkgs.runCommand "${archiveRoot}.tar.gz"
          {
            nativeBuildInputs = [
              pkgs.gnutar
              pkgs.gzip
            ];
          }
          ''
            mkdir -p "staging/${archiveRoot}"
            install -m755 ${package}/bin/agent-knowledge-client \
              "staging/${archiveRoot}/agent-knowledge-client"
            install -m644 ${./LICENSE} "staging/${archiveRoot}/LICENSE"
            install -m644 ${./README.md} "staging/${archiveRoot}/README.md"
            tar --sort=name --mtime='@1' --owner=0 --group=0 --numeric-owner \
              -czf "$out" -C staging "${archiveRoot}"
          '';
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
              license = pkgs.lib.licenses.asl20;
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
      clientMcpContainerRootFilesystemFor =
        system:
        let
          pkgs = pkgsFor.${system};
        in
        pkgs.runCommand "agent-knowledge-client-mcp-container-root" { } ''
          install -d "$out/etc" "$out/var/lib/agent-knowledge-client"
          install -m444 ${./deploy/container/client-mcp-passwd} "$out/etc/passwd"
          install -m444 ${./deploy/container/client-mcp-group} "$out/etc/group"
        '';
      clientMcpContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          package = clientMcpPackageFor system;
          rootFilesystem = clientMcpContainerRootFilesystemFor system;
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-client-mcp";
          tag = projectVersion;
          contents = [
            package
            rootFilesystem
          ];
          config = {
            User = "agent-knowledge-client";
            WorkingDir = "/var/lib/agent-knowledge-client";
            Entrypoint = [
              "${package}/bin/agent-knowledge-client"
              "mcp"
            ];
            Env = [ "HOME=/var/lib/agent-knowledge-client" ];
            StopSignal = "SIGTERM";
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge MCP Client";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
              "org.opencontainers.image.licenses" = projectLicense;
            };
          };
        };
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
              "org.opencontainers.image.licenses" = projectLicense;
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
              "org.opencontainers.image.licenses" = projectLicense;
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
              "org.opencontainers.image.licenses" = projectLicense;
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
              "org.opencontainers.image.licenses" = projectLicense;
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
            pkgs.coreutils
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
              "org.opencontainers.image.licenses" = projectLicense;
            };
          };
        };
      kubernetesE2eQuartzContainerImageFor =
        system:
        let
          pkgs = pkgsFor.${system};
          fixture = pkgs.runCommand "agent-knowledge-kubernetes-e2e-quartz-root" { } ''
            install -d "$out/fixture"
            install -m555 ${./deploy/kubernetes-e2e/build-site} \
              "$out/fixture/build-site"
          '';
        in
        pkgs.dockerTools.buildLayeredImage {
          name = "agent-knowledge-kubernetes-e2e-quartz";
          tag = projectVersion;
          contents = [
            pkgs.pkgsStatic.busybox
            fixture
          ];
          config = {
            User = "0";
            WorkingDir = "/";
            Entrypoint = [ "/bin/sh" ];
            Labels = {
              "org.opencontainers.image.title" = "Agent Knowledge Kubernetes E2E Quartz Fixture";
              "org.opencontainers.image.version" = projectVersion;
              "org.opencontainers.image.source" = "https://github.com/neodymium6/agent-knowledge";
              "org.opencontainers.image.licenses" = projectLicense;
            };
          };
        };
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor.${system};
          csiAttacherRbac = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/kubernetes-csi/external-attacher/v4.12.0/deploy/kubernetes/rbac.yaml";
            hash = "sha256-Oji1GYsElpJ8DOJsY+cdQ+ImPi27uTqDrN7HHXyQp2Y=";
          };
          csiExternalHealthMonitorRbac = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/kubernetes-csi/external-health-monitor/v0.18.0/deploy/kubernetes/external-health-monitor-controller/rbac.yaml";
            hash = "sha256-MgVZntqaJS4nN6yr4iWRr9jaNzwaq1J1zlQiOt/i0Sc=";
          };
          csiProvisionerRbac = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/kubernetes-csi/external-provisioner/v6.3.0/deploy/kubernetes/rbac.yaml";
            hash = "sha256-DuhCe3RqHTtpVwW3TC1/sWUSERCxoQwLbiBJGNk+gU8=";
          };
          csiResizerRbac = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/kubernetes-csi/external-resizer/v2.2.1/deploy/kubernetes/rbac.yaml";
            hash = "sha256-NhWvDQB9UeAuU81vdBX/e0GMOiHTO7bOUGY5IjYZzYI=";
          };
          csiSnapshotterRbac = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.6.0/deploy/kubernetes/csi-snapshotter/rbac-csi-snapshotter.yaml";
            hash = "sha256-3mf2ZBLxf6uYaElPZX/tQxnUwTd3uSEEWvwEEAvcnJg=";
          };
        in
        {
          default = pkgs.mkShell {
            AGENT_KNOWLEDGE_CSI_ATTACHER_RBAC = csiAttacherRbac;
            AGENT_KNOWLEDGE_CSI_EXTERNAL_HEALTH_MONITOR_RBAC = csiExternalHealthMonitorRbac;
            AGENT_KNOWLEDGE_CSI_HOSTPATH_SOURCE = csi-driver-host-path;
            AGENT_KNOWLEDGE_CSI_PROVISIONER_RBAC = csiProvisionerRbac;
            AGENT_KNOWLEDGE_CSI_RESIZER_RBAC = csiResizerRbac;
            AGENT_KNOWLEDGE_CSI_SNAPSHOTTER_RBAC = csiSnapshotterRbac;
            AGENT_KNOWLEDGE_EXTERNAL_SNAPSHOTTER_SOURCE = external-snapshotter;
            packages =
              with pkgs;
              [
                actionlint
                cargo
                cargo-deny
                clippy
                curl
                gh
                git
                jq
                just
                kube-linter
                kustomize
                nixfmt-tree
                pre-commit
                rustc
                rustfmt
                yq-go
              ]
              ++ lib.optionals stdenv.isLinux [
                kind
                kubectl
                openssh
                systemd
              ];
          };
        }
      );

      packages = forLinuxSystems (system: rec {
        agent-knowledge = packageFor system;
        agent-knowledge-client = staticClientPackageFor system;
        client-mcp-container-image = clientMcpContainerImageFor system;
        client-release-archive = clientReleaseArchiveFor system;
        gateway-container-image = gatewayContainerImageFor system;
        kubernetes-e2e-client = unwrappedPackageFor system;
        openssh-gateway-package = opensshGatewayPackageFor system;
        openssh-gateway-container-image = opensshGatewayContainerImageFor system;
        kubernetes-e2e-quartz-container-image = kubernetesE2eQuartzContainerImageFor system;
        storage-bootstrap-container-image = storageBootstrapContainerImageFor system;
        queue-ingress-container-image = queueIngressContainerImageFor system;
        systemd-e2e = import ./deploy/systemd/nixos-e2e.nix {
          pkgs = pkgsFor.${system};
          package = agent-knowledge;
        };
        worker-container-image = workerContainerImageFor system;
        default = agent-knowledge;
      });

      apps = forLinuxSystems (system: rec {
        agent-knowledge = {
          type = "app";
          program = "${packageFor system}/bin/agent-knowledge";
          meta.description = "Run Agent Knowledge";
        };
        agent-knowledge-client = {
          type = "app";
          program = "${staticClientPackageFor system}/bin/agent-knowledge-client";
          meta.description = "Run the Agent Knowledge client";
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
          kubernetesE2eQuartzContainerImage = kubernetesE2eQuartzContainerImageFor system;
        in
        {
          package = package;
          client-static =
            let
              client = staticClientPackageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-client-static"
              {
                nativeBuildInputs = [ pkgs.binutils ];
              }
              ''
                test "$(
                  ${client}/bin/agent-knowledge-client --version
                )" = "agent-knowledge-client ${projectVersion}"
                ! readelf -l ${client}/bin/agent-knowledge-client \
                  | grep -F 'INTERP'
                touch "$out"
              '';
          client-release-archive =
            let
              archive = clientReleaseArchiveFor system;
              archiveRoot = "agent-knowledge-client-v${projectVersion}-${clientRustTarget.${system}}";
            in
            pkgs.runCommand "check-agent-knowledge-client-release-archive"
              {
                nativeBuildInputs = [
                  pkgs.binutils
                  pkgs.gnutar
                  pkgs.gzip
                ];
              }
              ''
                mkdir extracted
                tar -xzf ${archive} -C extracted
                test -x "extracted/${archiveRoot}/agent-knowledge-client"
                test -f "extracted/${archiveRoot}/LICENSE"
                test -f "extracted/${archiveRoot}/README.md"
                test "$(
                  "extracted/${archiveRoot}/agent-knowledge-client" --version
                )" = "agent-knowledge-client ${projectVersion}"
                ! readelf -l \
                  "extracted/${archiveRoot}/agent-knowledge-client" \
                  | grep -F 'INTERP'
                touch "$out"
              '';
          client-mcp-container-image =
            let
              clientMcpContainerImage = clientMcpContainerImageFor system;
              clientMcpPackage = clientMcpPackageFor system;
            in
            pkgs.runCommand "check-agent-knowledge-client-mcp-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/container/check-image.sh} \
                      ${clientMcpContainerImage} \
                      ${containerArchitecture.${system}} \
                      ${clientMcpPackage}/bin/agent-knowledge-client \
                      ${projectVersion} \
                      ${./deploy/container/client-mcp-passwd} \
                      ${./deploy/container/client-mcp-group} \
                      agent-knowledge-client-mcp \
                      agent-knowledge-client \
                      mcp \
                      - \
                      /var/lib/agent-knowledge-client \
                      "Agent Knowledge MCP Client" \
                      -
                    touch "$out"
              '';
          package-metadata =
            assert package.pname == "agent-knowledge";
            assert package.version == projectVersion;
            pkgs.runCommand "check-agent-knowledge-package-metadata" { } ''
              touch "$out"
            '';
          container-image = workerContainerImageCheck;
          kubernetes-manifests = kubernetesManifestCheck;
          kubernetes-e2e-quartz-container-image =
            pkgs.runCommand "check-agent-knowledge-kubernetes-e2e-quartz-container-image"
              {
                nativeBuildInputs = [
                  pkgs.gnutar
                  pkgs.gzip
                  pkgs.jq
                ];
              }
              ''
                ${pkgs.bash}/bin/bash ${./deploy/kubernetes-e2e/check-quartz-image.sh} \
                  ${kubernetesE2eQuartzContainerImage} \
                  ${containerArchitecture.${system}} \
                  ${projectVersion}
                touch "$out"
              '';
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
