{ pkgs, package }:

let
  gatewayUid = 41003;
  quartzFixture = pkgs.writeShellApplication {
    name = "build-site";
    runtimeInputs = [ pkgs.coreutils ];
    text = ''
      if [ "$#" -ne 5 ] || [ "$1" != build ] || [ "$2" != -d ] || [ "$4" != -o ]; then
        exit 2
      fi

      test -d "$3"
      test -d "$5"
      printf '%s\n' '<p>fictional systemd E2E site</p>' >"$5/index.html"
    '';
  };
  workerConfiguration = pkgs.writeText "fictional-worker.yaml" ''
    schema_version: 1
    storage:
      queue_root: /var/lib/agent-knowledge/queue
      repository_root: /var/lib/agent-knowledge/repository
      content_root: /var/lib/agent-knowledge/content
      work_root: /var/lib/agent-knowledge/work
      release_root: /var/lib/agent-knowledge/releases
    repository:
      official_branch: main
      author_name: Fictional Knowledge Worker
      author_email: worker@example.invalid
    quartz:
      program: /opt/fictional-quartz/bin/build-site
      integration_root: /opt/fictional-quartz
      timeout_seconds: 30
    batch:
      debounce_seconds: 1
      maximum_age_seconds: 5
      maximum_scan_entries: 100
      maximum_requests: 10
      maximum_recovery_requests: 100
    retention:
      retained_releases: 3
      maximum_scan_entries: 100
      maximum_removals: 10
  '';
  gatewayConfiguration = pkgs.writeText "fictional-gateway.yaml" ''
    schema_version: 4
    identity:
      gateway_uid: ${toString gatewayUid}
    storage:
      queue_socket: /run/agent-knowledge/queue-ingress.sock
      git_directory: /var/lib/agent-knowledge/repository
      content_root: /var/lib/agent-knowledge/content
    repository:
      official_branch: main
    reads:
      maximum_results: 100
      maximum_query_characters: 512
      maximum_index_entries: 1000
      maximum_index_markdown_bytes: 10485760
      maximum_search_documents: 1000
      maximum_search_markdown_bytes: 10485760
      operation_timeout_seconds: 30
      maximum_response_bytes: 10485760
      search_metadata:
        node: true
        agent: true
        session: true
        request_id: true
    transport:
      submit_timeout_seconds: 30
  '';
  requestPackage =
    {
      name,
      requestId,
      documentId,
      sessionId,
      marker,
    }:
    pkgs.runCommand name { } ''
      mkdir -p "$out/payload/benchmark"
      cat >"$out/request.json" <<'EOF'
      {
        "protocol_version": 1,
        "request_id": "${requestId}",
        "title": "Record fictional systemd benchmark ${marker}",
        "project": "fictional-solver",
        "document_type": "experiment",
        "node": "fictional-systemd-node",
        "agent": "codex",
        "session": "${sessionId}",
        "created_at": "2026-08-04T00:00:00Z",
        "operations": [
          {
            "type": "create_document",
            "document_id": "${documentId}",
            "content": "benchmark/index.md"
          },
          {
            "type": "add_attachment",
            "document_id": "${documentId}",
            "source": "benchmark/results.csv",
            "name": "results.csv"
          }
        ]
      }
      EOF
      cat >"$out/payload/benchmark/index.md" <<'EOF'
      ---
      schema_version: 1
      document_id: ${documentId}
      title: Fictional systemd benchmark ${marker}
      created: 2026-08-04T00:00:00Z
      node: fictional-systemd-node
      agent: codex
      session: ${sessionId}
      request_id: ${requestId}
      tags:
        - systemd
        - fictional
      status: active
      ---

      Fictional systemd benchmark body ${marker}.
      EOF
      printf '%s\n' 'phase,value' 'systemd-${marker},42' \
        >"$out/payload/benchmark/results.csv"
    '';
  firstRequest = requestPackage {
    name = "fictional-systemd-request-one";
    requestId = "01K00000000000000000000010";
    documentId = "01K00000000000000000000011";
    sessionId = "01K00000000000000000000012";
    marker = "one";
  };
  secondRequest = requestPackage {
    name = "fictional-systemd-request-two";
    requestId = "01K00000000000000000000020";
    documentId = "01K00000000000000000000021";
    sessionId = "01K00000000000000000000022";
    marker = "two";
  };
in
pkgs.testers.runNixOSTest {
  name = "agent-knowledge-systemd-e2e";
  requiredFeatures.kvm = false;

  nodes.machine =
    { lib, ... }:
    {
      virtualisation.memorySize = 2048;
      virtualisation.diskSize = 4096;
      virtualisation.cores = 2;
      virtualisation.graphics = false;
      networking.useDHCP = false;

      environment.systemPackages = [
        package
        pkgs.git
        pkgs.gnutar
        pkgs.jq
        pkgs.openssh
      ];

      environment.etc."agent-knowledge/worker.yaml" = {
        source = workerConfiguration;
        mode = "0444";
      };
      environment.etc."agent-knowledge/gateway.yaml" = {
        source = gatewayConfiguration;
        mode = "0444";
      };

      users.mutableUsers = false;
      users.groups = {
        agent-knowledge.gid = 41001;
        agent-knowledge-queue.gid = 41002;
        agent-knowledge-gateway.gid = 41003;
        agent-knowledge-ingress.gid = 41004;
      };
      users.users = {
        agent-knowledge = {
          isSystemUser = true;
          uid = 41001;
          group = "agent-knowledge";
          extraGroups = [ "agent-knowledge-queue" ];
          home = "/var/lib/agent-knowledge";
          createHome = false;
        };
        agent-knowledge-queue = {
          isSystemUser = true;
          uid = 41002;
          group = "agent-knowledge-queue";
          home = "/var/lib/agent-knowledge";
          createHome = false;
        };
        fictional-ak-gateway = {
          isSystemUser = true;
          uid = gatewayUid;
          group = "agent-knowledge-gateway";
          extraGroups = [ "agent-knowledge-ingress" ];
          home = "/var/empty";
          createHome = false;
          # OpenSSH rejects public-key authentication for locked accounts. This
          # syntactically valid fixture hash keeps the account unlocked without
          # providing a usable password; password authentication is disabled.
          hashedPassword = "$6$fictional$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
          shell = "${package}/bin/agent-knowledge-ssh-shell";
        };
      };

      systemd.packages = [ package ];
      systemd.services.agent-knowledge-storage-bootstrap = {
        description = "Initialize fictional Agent Knowledge E2E storage";
        after = [ "local-fs.target" ];
        before = [
          "agent-knowledge-worker.service"
          "agent-knowledge-queue-ingress.socket"
        ];
        unitConfig.DefaultDependencies = false;
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          install -d -m 0755 -o root -g root /opt/fictional-quartz/bin
          install -m 0555 -o root -g root \
            ${quartzFixture}/bin/build-site \
            /opt/fictional-quartz/bin/build-site
          install -d -m 0751 -o root -g agent-knowledge-queue \
            /var/lib/agent-knowledge
          install -d -m 2750 -o agent-knowledge-queue \
            -g agent-knowledge-ingress /run/agent-knowledge
          ${package}/bin/agent-knowledge admin bootstrap-storage \
            --config /etc/agent-knowledge/worker.yaml \
            --gateway-owner fictional-ak-gateway
          ${pkgs.systemd}/bin/systemd-tmpfiles --create \
            ${package}/lib/tmpfiles.d/agent-knowledge.conf
        '';
      };
      systemd.services.agent-knowledge-worker = {
        requires = [ "agent-knowledge-storage-bootstrap.service" ];
        after = [ "agent-knowledge-storage-bootstrap.service" ];
      };
      systemd.sockets.agent-knowledge-queue-ingress = {
        requires = [ "agent-knowledge-storage-bootstrap.service" ];
        after = [ "agent-knowledge-storage-bootstrap.service" ];
      };

      services.openssh = {
        enable = true;
        ports = [ 2222 ];
        authorizedKeysFiles = lib.mkForce [ "/etc/agent-knowledge/authorized_keys" ];
        hostKeys = [
          {
            path = "/etc/ssh/ssh_host_ed25519_key";
            type = "ed25519";
          }
        ];
        settings = {
          AuthenticationMethods = "publickey";
          KbdInteractiveAuthentication = false;
          PasswordAuthentication = false;
          PermitRootLogin = "no";
        };
        extraConfig = ''
          AllowUsers fictional-ak-gateway
          AllowGroups agent-knowledge-gateway
          PermitTTY no
          DisableForwarding yes
          AllowAgentForwarding no
          AllowTcpForwarding no
          GatewayPorts no
          X11Forwarding no
          PermitTunnel no
          PermitUserEnvironment no
          PermitUserRC no
          MaxAuthTries 3
          MaxSessions 1
          StrictModes yes
        '';
      };
    };

  testScript = ''
    import json

    client = "${package}/bin/agent-knowledge"
    jq = "${pkgs.jq}/bin/jq"

    def run_client(arguments):
        return machine.succeed(client + " client " + arguments)

    def wait_for_completed(request_id):
        machine.wait_until_succeeds(
            client
            + " client status --destination fictional-systemd "
            + "--request-id "
            + request_id
            + " --timeout-seconds 60 | "
            + jq
            + " -e '.status == \"completed\"' >/dev/null",
            timeout=60,
        )

    machine.start(allow_reboot=True)
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("sshd.service")
    machine.wait_for_open_port(2222)
    machine.succeed(
        "systemctl start agent-knowledge-queue-ingress.socket "
        "agent-knowledge-worker.service"
    )
    machine.wait_for_unit("agent-knowledge-storage-bootstrap.service")
    machine.wait_for_unit("agent-knowledge-worker.service")
    machine.wait_for_unit("agent-knowledge-queue-ingress.socket")

    machine.succeed(
        "test \"$(id -G agent-knowledge)\" = '41001 41002'"
    )
    machine.succeed(
        "test \"$(id -G agent-knowledge-queue)\" = '41002'"
    )
    machine.succeed(
        "test \"$(id -G fictional-ak-gateway)\" = '41003 41004'"
    )
    machine.succeed(
        "systemctl cat agent-knowledge-worker.service "
        "| grep -F 'ProtectSystem=strict'"
    )
    machine.succeed(
        "systemctl cat agent-knowledge-queue-ingress@.service "
        "| grep -F 'PrivateNetwork=yes'"
    )

    machine.succeed(
        "install -d -m 0700 /root/.ssh; "
        "ssh-keygen -q -t ed25519 -N \"\" "
        "-C fictional-systemd-client -f /root/.ssh/id_ed25519"
    )
    machine.succeed(
        "public_key=$(cat /root/.ssh/id_ed25519.pub); "
        "printf '%s %s\\n' "
        "'restrict,command=\"akg-v1 /etc/agent-knowledge/gateway.yaml "
        "fictional-systemd-node\"' \"$public_key\" "
        ">/etc/agent-knowledge/authorized_keys; "
        "chmod 0644 /etc/agent-knowledge/authorized_keys"
    )
    machine.succeed(
        "host_key=$(cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub); "
        "printf '[127.0.0.1]:2222 %s\\n' \"$host_key\" "
        ">/root/.ssh/known_hosts; chmod 0644 /root/.ssh/known_hosts"
    )
    machine.succeed(
        "printf '%s\\n' "
        "'Host fictional-systemd' "
        "'  HostName 127.0.0.1' "
        "'  Port 2222' "
        "'  User fictional-ak-gateway' "
        "'  IdentityFile /root/.ssh/id_ed25519' "
        "'  IdentitiesOnly yes' "
        "'  BatchMode yes' "
        "'  StrictHostKeyChecking yes' "
        "'  UserKnownHostsFile /root/.ssh/known_hosts' "
        "'  GlobalKnownHostsFile /dev/null' "
        "'  RequestTTY no' "
        ">/root/.ssh/config; chmod 0600 /root/.ssh/config"
    )

    initial = json.loads(
        run_client(
            "list --destination fictional-systemd "
            "--maximum-results 10 --timeout-seconds 60"
        )
    )
    assert initial["documents"] == []

    accepted = json.loads(
        run_client(
            "submit --destination fictional-systemd "
            "--package-root ${firstRequest} --timeout-seconds 60"
        )
    )
    assert accepted["status"] == "accepted"
    wait_for_completed("01K00000000000000000000010")

    first_document = json.loads(
        run_client(
            "get --destination fictional-systemd "
            "--document-id 01K00000000000000000000011 "
            "--timeout-seconds 60"
        )
    )
    assert "Fictional systemd benchmark body one." in first_document["document"]["markdown"]
    search = json.loads(
        run_client(
            "search --destination fictional-systemd --query 'body one' "
            "--maximum-results 10 --timeout-seconds 60"
        )
    )
    assert [document["metadata"]["document_id"] for document in search["documents"]] == [
        "01K00000000000000000000011"
    ]
    machine.succeed(
        client
        + " client export --destination fictional-systemd "
        + "--document-id 01K00000000000000000000011 "
        + "--timeout-seconds 60 >/tmp/first.tar"
    )
    machine.succeed("tar -xOf /tmp/first.tar results.csv | grep -F 'systemd-one,42'")
    machine.succeed("test -f /var/lib/agent-knowledge/releases/current/index.html")

    machine.succeed("systemctl restart agent-knowledge-worker.service")
    machine.wait_for_unit("agent-knowledge-worker.service")
    run_client(
        "get --destination fictional-systemd "
        "--document-id 01K00000000000000000000011 --timeout-seconds 60"
    )

    machine.reboot()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("sshd.service")
    machine.wait_for_open_port(2222)
    machine.succeed(
        "systemctl start agent-knowledge-queue-ingress.socket "
        "agent-knowledge-worker.service"
    )
    machine.wait_for_unit("agent-knowledge-storage-bootstrap.service")
    machine.wait_for_unit("agent-knowledge-worker.service")
    machine.wait_for_unit("agent-knowledge-queue-ingress.socket")
    machine.succeed("test -S /run/agent-knowledge/queue-ingress.sock")

    persisted = json.loads(
        run_client(
            "get --destination fictional-systemd "
            "--document-id 01K00000000000000000000011 "
            "--timeout-seconds 60"
        )
    )
    assert "Fictional systemd benchmark body one." in persisted["document"]["markdown"]

    accepted_after_reboot = json.loads(
        run_client(
            "submit --destination fictional-systemd "
            "--package-root ${secondRequest} --timeout-seconds 60"
        )
    )
    assert accepted_after_reboot["status"] == "accepted"
    wait_for_completed("01K00000000000000000000020")

    final_list = json.loads(
        run_client(
            "list --destination fictional-systemd "
            "--maximum-results 10 --timeout-seconds 60"
        )
    )
    assert {
        document["metadata"]["document_id"] for document in final_list["documents"]
    } == {
        "01K00000000000000000000011",
        "01K00000000000000000000021",
    }
    machine.succeed(
        "test \"$(git --git-dir=/var/lib/agent-knowledge/repository "
        "rev-list --count main)\" = 3"
    )
  '';
}
