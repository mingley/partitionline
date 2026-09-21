"""
Tests for the three-broker KRaft fault fixture (KL03-17).

Validates:
- Pinned official images, roles, ports, feature levels, and topology manifest.
- Explicit failure on missing Docker, Java, or operator approval.
- Start, status, stop, restart, disconnect, reconnect, and leader-movement controls
  address only recorded owned resources.
- Quorum health and min-ISR status evaluation under node failures and disconnections.
- Safe cleanup that purges only recorded owned resources and never kills processes
  by name or touches unowned topics.
- Completely isolated execution using the fake backend.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "cluster-fixture.sh"
TOPOLOGY_PATH = REPO_ROOT / "tests" / "cluster" / "topology.json"


class TestTopologyManifest(unittest.TestCase):
    """Validates the pinned topology manifest (tests/cluster/topology.json)."""

    def setUp(self):
        self.assertTrue(TOPOLOGY_PATH.exists(), f"Topology file missing at {TOPOLOGY_PATH}")
        with open(TOPOLOGY_PATH, "r") as f:
            self.top = json.load(f)

    def test_pinned_replication_and_isr(self):
        self.assertEqual(self.top["replication_factor"], 3)
        self.assertEqual(self.top["min_insync_replicas"], 2)
        configs = self.top.get("configs", {})
        self.assertEqual(configs.get("default.replication.factor"), 3)
        self.assertEqual(configs.get("min.insync.replicas"), 2)
        self.assertEqual(configs.get("offsets.topic.replication.factor"), 3)
        self.assertEqual(configs.get("transaction.state.log.replication.factor"), 3)
        self.assertEqual(configs.get("transaction.state.log.min.isr"), 2)
        self.assertEqual(configs.get("share.coordinator.state.topic.replication.factor"), 3)
        self.assertEqual(configs.get("share.coordinator.state.topic.min.isr"), 2)

    def test_pinned_image_and_features(self):
        self.assertEqual(self.top["image"], "apache/kafka:4.1.0")
        self.assertIn("apache/kafka:3.9.1", self.top.get("alternative_images", []))
        features = self.top.get("feature_levels", {})
        self.assertEqual(features.get("share.version"), 1)
        self.assertEqual(features.get("metadata.version"), "4.1-IV0")
        self.assertEqual(features.get("group.version"), 1)
        self.assertEqual(features.get("transaction.version"), 2)

    def test_nodes_roles_and_ports(self):
        nodes = self.top.get("nodes", [])
        self.assertEqual(len(nodes), 3)
        expected_ports = {
            1: (19092, 19093),
            2: (19094, 19095),
            3: (19096, 19097),
        }
        for n in nodes:
            nid = n["node_id"]
            self.assertIn(nid, expected_ports)
            self.assertIn("broker", n["roles"])
            self.assertIn("controller", n["roles"])
            client_port, controller_port = expected_ports[nid]
            self.assertEqual(n["client_port"], client_port)
            self.assertEqual(n["controller_port"], controller_port)
            self.assertIn(f":{client_port}", n["client_listener"])
            self.assertIn(f":{controller_port}", n["controller_listener"])


class TestClusterFixtureCLI(unittest.TestCase):
    """Tests the CLI script scripts/cluster-fixture.sh."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="test-cluster-fixture-")
        self.state_dir = Path(self.temp_dir.name) / "cluster-state"
        self.base_env = dict(os.environ)
        self.base_env["CLUSTER_STATE_DIR"] = str(self.state_dir)
        self.base_env["TOPOLOGY_FILE"] = str(TOPOLOGY_PATH)

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_cmd(self, *args, env=None, check=True):
        cmd = ["bash", str(SCRIPT_PATH)] + list(args)
        run_env = dict(self.base_env)
        if env:
            run_env.update(env)
        proc = subprocess.run(
            cmd,
            env=run_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr
            )
        return proc

    def test_manifest_command(self):
        proc = self.run_cmd("manifest")
        manifest = json.loads(proc.stdout)
        self.assertEqual(manifest["replication_factor"], 3)
        self.assertEqual(manifest["min_insync_replicas"], 2)
        self.assertEqual(len(manifest["nodes"]), 3)

    def test_missing_approval_fails_explicitly(self):
        # Docker without approval
        proc = self.run_cmd(
            "--backend", "docker", "start",
            env={"CLUSTER_FIXTURE_APPROVED": "0"},
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("real cluster execution requires explicit approval", proc.stderr)

        # Native without approval
        proc = self.run_cmd(
            "--backend", "native", "start",
            env={"CLUSTER_FIXTURE_APPROVED": "0"},
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("real cluster execution requires explicit approval", proc.stderr)

    def test_missing_docker_binary_fails_explicitly(self):
        proc = self.run_cmd(
            "--backend", "docker", "--approved", "start",
            env={"DOCKER_BIN": "nonexistent_docker_binary"},
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("docker is required for docker backend but not found", proc.stderr)

    def test_missing_java_binary_fails_explicitly(self):
        proc = self.run_cmd(
            "--backend", "native", "--approved", "start",
            env={"JAVA_BIN": "nonexistent_java_binary"},
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("java is required for native backend but not found", proc.stderr)

    def test_fake_backend_lifecycle(self):
        # 1. Start all nodes
        self.run_cmd("--backend", "fake", "start")
        resources_file = self.state_dir / "resources.json"
        self.assertTrue(resources_file.exists())
        with open(resources_file) as f:
            res = json.load(f)
        self.assertEqual(res["backend"], "fake")
        self.assertEqual(len(res["nodes"]), 3)
        for nid in ("1", "2", "3"):
            self.assertEqual(res["nodes"][nid]["state"], "running")
            self.assertIsNotNone(res["nodes"][nid]["pid"])

        # 2. Check status
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        status = json.loads(proc.stdout)
        self.assertTrue(status["quorum_healthy"])
        self.assertTrue(status["min_isr_met"])
        self.assertEqual(sorted(status["running_nodes"]), [1, 2, 3])

        # 3. Stop single node (node 2)
        self.run_cmd("--backend", "fake", "stop", "--node", "2")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        status = json.loads(proc.stdout)
        self.assertTrue(status["quorum_healthy"])  # 2 of 3 running is majority
        self.assertTrue(status["min_isr_met"])     # min_isr=2 is satisfied
        self.assertEqual(sorted(status["running_nodes"]), [1, 3])
        self.assertEqual(status["stopped_nodes"], [2])

        # 4. Restart node 2
        self.run_cmd("--backend", "fake", "restart", "--node", "2")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        status = json.loads(proc.stdout)
        self.assertEqual(sorted(status["running_nodes"]), [1, 2, 3])

        # 5. Stop all nodes
        self.run_cmd("--backend", "fake", "stop")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        status = json.loads(proc.stdout)
        self.assertFalse(status["quorum_healthy"])
        self.assertEqual(status["running_nodes"], [])
        self.assertEqual(sorted(status["stopped_nodes"]), [1, 2, 3])

        # 6. Cleanup removes state
        self.run_cmd("--backend", "fake", "cleanup")
        self.assertFalse(self.state_dir.exists())

    def test_fault_controls_and_quorum_degradation(self):
        self.run_cmd("--backend", "fake", "start")

        # Disconnect node 1
        self.run_cmd("--backend", "fake", "disconnect", "--node", "1")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertEqual(st["disconnected_nodes"], [1])
        self.assertEqual(sorted(st["running_nodes"]), [2, 3])
        self.assertTrue(st["quorum_healthy"])  # 2 of 3 running

        # Disconnecting another node drops running nodes below quorum majority
        self.run_cmd("--backend", "fake", "disconnect", "--node", "2")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertEqual(sorted(st["disconnected_nodes"]), [1, 2])
        self.assertEqual(st["running_nodes"], [3])
        self.assertFalse(st["quorum_healthy"])  # only 1 running node -> quorum lost
        self.assertFalse(st["min_isr_met"])     # min_isr=2 violated

        # Cannot disconnect an already disconnected node
        proc = self.run_cmd("--backend", "fake", "disconnect", "--node", "1", check=False)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("must be running", proc.stderr)

        # Reconnect node 1
        self.run_cmd("--backend", "fake", "reconnect", "--node", "1")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertEqual(sorted(st["running_nodes"]), [1, 3])
        self.assertTrue(st["quorum_healthy"])
        self.assertTrue(st["min_isr_met"])

        # Reconnect node 2
        self.run_cmd("--backend", "fake", "reconnect", "--node", "2")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertEqual(sorted(st["running_nodes"]), [1, 2, 3])
        self.assertEqual(st["disconnected_nodes"], [])

        self.run_cmd("--backend", "fake", "cleanup")

    def test_topic_ownership_and_leader_movement(self):
        self.run_cmd("--backend", "fake", "start")

        # Create owned topic
        self.run_cmd("--backend", "fake", "create-topic", "--topic", "pl-test-events")
        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertIn("pl-test-events", st["topics_owned"])
        self.assertEqual(st["partition_leaders"]["pl-test-events:0"], 1)

        # Move leader to node 2
        proc = self.run_cmd(
            "--backend", "fake", "leader-move",
            "--topic", "pl-test-events",
            "--partition", "0",
            "--to-node", "2",
        )
        move_res = json.loads(proc.stdout)
        self.assertEqual(move_res["new_leader"], 2)

        proc = self.run_cmd("--backend", "fake", "status", "--json")
        st = json.loads(proc.stdout)
        self.assertEqual(st["partition_leaders"]["pl-test-events:0"], 2)

        # Stop node 3, attempting to move leader to node 3 must fail
        self.run_cmd("--backend", "fake", "stop", "--node", "3")
        proc = self.run_cmd(
            "--backend", "fake", "leader-move",
            "--topic", "pl-test-events",
            "--partition", "0",
            "--to-node", "3",
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("cannot move leader to non-running node", proc.stderr)

        self.run_cmd("--backend", "fake", "cleanup")

    def test_cleanup_addresses_only_owned_resources(self):
        # 1. Start cluster
        self.run_cmd("--backend", "fake", "start")
        self.run_cmd("--backend", "fake", "create-topic", "--topic", "pl-owned-topic-A")

        resources_file = self.state_dir / "resources.json"
        self.assertTrue(resources_file.exists())
        with open(resources_file) as f:
            res = json.load(f)

        # Verify resources.json has exact recorded container names and PIDs
        self.assertEqual(len(res["nodes"]), 3)
        self.assertEqual(res["topics_owned"], ["pl-owned-topic-A"])
        for nid, n in res["nodes"].items():
            self.assertTrue(n["container_name"].startswith("pl-cluster-node-"))
            self.assertIsInstance(n["pid"], int)

        # 2. Cleanup
        proc = self.run_cmd("--backend", "fake", "cleanup")
        self.assertIn("only recorded owned resources were removed", proc.stdout)
        self.assertFalse(self.state_dir.exists())

        # 3. Repeated cleanup is idempotent and safe
        proc = self.run_cmd("--backend", "fake", "cleanup")
        self.assertIn("no recorded resources found", proc.stdout)

    def test_script_self_test_flag(self):
        proc = self.run_cmd("--self-test")
        self.assertIn("self-test: ALL CHECKS PASSED (isolated fake backend)", proc.stdout)


if __name__ == "__main__":
    unittest.main()
