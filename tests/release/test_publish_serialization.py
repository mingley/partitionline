"""
Unit tests for publisher retirement, release serialization, and eligibility parity (KL08-03).

Validates:
- Version-0.1.0 first-publish workflow is retired, fails closed on dispatch, and has no publish job.
- Release workflow uses a shared non-cancelling release lock (group: release-publish-lock, cancel-in-progress: false).
- Competing tags and same-tag reruns serialize without cancelling in-flight publishes.
- Interrupted publish, confirmation, and release-note steps are idempotent.
- Owner/local publication path (owner-publish.sh) enforces the same eligibility gates as release.yml.
- Release-plz remains strictly PR-only.
- All tests run offline without network access to crates.io or GitHub.
"""

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
FIRST_PUBLISH_YML = WORKFLOWS_DIR / "first-publish.yml"
RELEASE_YML = WORKFLOWS_DIR / "release.yml"
RELEASE_PLZ_YML = WORKFLOWS_DIR / "release-plz.yml"
OWNER_PUBLISH_SH = REPO_ROOT / "scripts" / "owner-publish.sh"
REHEARSE_SH = REPO_ROOT / "scripts" / "rehearse-partial-release.sh"


class TestFirstPublishRetired(unittest.TestCase):
    """Verifies that .github/workflows/first-publish.yml is retired and fails closed."""

    def setUp(self):
        self.assertTrue(FIRST_PUBLISH_YML.is_file(), f"Missing {FIRST_PUBLISH_YML}")
        self.text = FIRST_PUBLISH_YML.read_text(encoding="utf-8")

    def test_workflow_dispatch_fails_closed(self):
        """Workflow dispatch must exist but trigger a fail-closed step."""
        self.assertIn("workflow_dispatch:", self.text)
        self.assertIn("exit 1", self.text)
        self.assertRegex(self.text, r"(?i)retired|obsolete")

    def test_no_cargo_publish_in_first_publish(self):
        """first-publish.yml must contain NO cargo publish command."""
        self.assertNotRegex(
            self.text,
            r"(?m)^\s*cargo\s+publish",
            "first-publish.yml must not contain cargo publish",
        )

    def test_shares_release_lock(self):
        """first-publish.yml must participate in the shared release lock with cancel-in-progress: false."""
        group_match = re.search(r"(?m)^\s*group:\s*([^\s]+)", self.text)
        self.assertIsNotNone(group_match, "concurrency.group must be defined")
        self.assertEqual(
            group_match.group(1),
            "release-publish-lock",
            "first-publish.yml must share the release-publish-lock",
        )
        cip_match = re.search(r"(?m)^\s*cancel-in-progress:\s*([^\s]+)", self.text)
        self.assertIsNotNone(cip_match, "cancel-in-progress must be defined")
        self.assertEqual(
            cip_match.group(1).lower(),
            "false",
            "cancel-in-progress must be false to avoid cancelling in-flight runs",
        )


class TestReleaseWorkflowSerializationAndLocking(unittest.TestCase):
    """Verifies release.yml concurrency and lock serialization design."""

    def setUp(self):
        self.assertTrue(RELEASE_YML.is_file(), f"Missing {RELEASE_YML}")
        self.text = RELEASE_YML.read_text(encoding="utf-8")

    def test_shared_non_cancelling_release_lock(self):
        """release.yml must use a shared non-cancelling release lock."""
        group_match = re.search(r"(?m)^\s*group:\s*([^\s]+)", self.text)
        self.assertIsNotNone(group_match, "concurrency.group must be defined")
        group_expr = group_match.group(1)

        # Must NOT be parameterized by ref or tag (which would cause competing tags to run concurrently)
        self.assertNotIn(
            "github.ref",
            group_expr,
            "concurrency.group must not depend on github.ref (must serialize across different tags)",
        )
        self.assertNotIn(
            "inputs.tag",
            group_expr,
            "concurrency.group must not depend on inputs.tag (must serialize across different tags)",
        )
        self.assertEqual(
            group_expr,
            "release-publish-lock",
            "concurrency.group must be the shared release-publish-lock",
        )

        cip_match = re.search(r"(?m)^\s*cancel-in-progress:\s*([^\s]+)", self.text)
        self.assertIsNotNone(cip_match, "cancel-in-progress must be defined")
        self.assertEqual(
            cip_match.group(1).lower(),
            "false",
            "cancel-in-progress must be false so in-progress publish is never killed",
        )

    def test_competing_tags_resolve_to_same_lock(self):
        """Simulate two competing tags and prove they resolve to the same concurrency group."""
        group_match = re.search(r"(?m)^\s*group:\s*([^\s]+)", self.text)
        self.assertIsNotNone(group_match)
        lock_name = group_match.group(1)

        def resolve_lock(ref_name, dispatch_tag=None):
            # If the group expression has no tag/ref variables, it is a constant shared lock
            resolved = lock_name.replace("${{ github.ref }}", ref_name)
            if dispatch_tag:
                resolved = resolved.replace("${{ github.event.inputs.tag }}", dispatch_tag)
            return resolved

        tag1_lock = resolve_lock("refs/tags/v0.1.1")
        tag2_lock = resolve_lock("refs/tags/v0.1.2")
        dispatch_lock = resolve_lock("refs/heads/main", dispatch_tag="v0.1.3")

        self.assertEqual(tag1_lock, tag2_lock, "Competing tags must resolve to the identical lock group")
        self.assertEqual(tag1_lock, dispatch_lock, "Tag push and dispatch must resolve to the identical lock group")
        self.assertEqual(tag1_lock, "release-publish-lock")


class TestReleaseWorkflowIdempotence(unittest.TestCase):
    """Verifies that release.yml steps are idempotent on retry or rerun."""

    def setUp(self):
        self.text = RELEASE_YML.read_text(encoding="utf-8")

    def test_soft_skip_step_detects_already_published(self):
        """Soft-skip step must output skip=1 if the crate is already on crates.io."""
        self.assertIn("id: already", self.text)
        self.assertIn("skip=1", self.text)
        self.assertIn("skip=0", self.text)

    def test_publish_step_gated_on_skip_output(self):
        """Authenticate and Publish steps must be skipped when already published."""
        auth_gated = re.search(
            r"- name: Authenticate to crates\.io.*?\n\s+if:\s*steps\.already\.outputs\.skip\s*!=\s*'1'",
            self.text,
            re.DOTALL,
        )
        self.assertIsNotNone(auth_gated, "Authenticate step must be gated on skip != '1'")

        pub_gated = re.search(
            r"- name: Publish.*?\n\s+if:\s*steps\.already\.outputs\.skip\s*!=\s*'1'",
            self.text,
            re.DOTALL,
        )
        self.assertIsNotNone(pub_gated, "Publish step must be gated on skip != '1'")

    def test_confirm_step_idempotent_on_retry(self):
        """Confirm step must handle skip=1 without redundant polling or failure."""
        confirm_step = re.search(
            r"- name: Confirm crates\.io\n\s+run:\s*\|\n(.*?)(?=\n\s+- name:|\Z)",
            self.text,
            re.DOTALL,
        )
        self.assertIsNotNone(confirm_step, "Confirm crates.io step must exist")
        body = confirm_step.group(1)
        self.assertIn("steps.already.outputs.skip", body, "Confirm step should recognize skipped publish")

    def test_release_notes_idempotent_if_release_exists(self):
        """GitHub Release notes step must check gh release view before gh release create."""
        notes_step = re.search(
            r"- name: GitHub Release notes\n.*?\n\s+run:\s*\|\n(.*?)(?=\n\s+- name:|\Z)",
            self.text,
            re.DOTALL,
        )
        self.assertIsNotNone(notes_step, "GitHub Release notes step must exist")
        body = notes_step.group(1)
        self.assertIn("gh release view", body, "Must check if release already exists")
        self.assertIn("gh release create", body, "Must create release only if missing")


class TestOwnerPublishEligibilityParity(unittest.TestCase):
    """Verifies that scripts/owner-publish.sh implements the same eligibility logic as release.yml."""

    def setUp(self):
        self.assertTrue(OWNER_PUBLISH_SH.is_file(), f"Missing {OWNER_PUBLISH_SH}")
        self.text = OWNER_PUBLISH_SH.read_text(encoding="utf-8")

    def test_refuses_prerelease_and_non_final_versions(self):
        """owner-publish.sh must refuse prerelease versions, matching release.yml."""
        self.assertIn("*-*|*+*", self.text)
        self.assertIn("^[0-9]+\\.[0-9]+\\.[0-9]+$", self.text)

    def test_requires_main_branch_and_clean_tree(self):
        """owner-publish.sh must verify clean working tree on main branch."""
        self.assertIn('branch="$(git rev-parse --abbrev-ref HEAD)"', self.text)
        self.assertIn('git status --porcelain', self.text)

    def test_skips_cargo_publish_when_already_on_crates_io(self):
        """owner-publish.sh must probe crates.io and skip cargo publish when present."""
        self.assertIn("pl_crates_probe_version", self.text)
        self.assertIn('if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then', self.text)
        self.assertIn("skipping cargo publish", self.text)

    def test_exact_sha_ci_and_publish_readiness_required(self):
        """owner-publish.sh must enforce exact-SHA CI and call ci-publish-ready.sh."""
        self.assertIn("check-main-ci.sh", self.text)
        self.assertIn("CHECK_SHA=", self.text)
        self.assertIn("REQUIRE_MAIN_CI=", self.text)
        self.assertIn("ci-publish-ready.sh", self.text)


class TestReleasePlzPrOnly(unittest.TestCase):
    """Verifies that release-plz is strictly PR-only and cannot publish."""

    def setUp(self):
        self.assertTrue(RELEASE_PLZ_YML.is_file(), f"Missing {RELEASE_PLZ_YML}")
        self.text = RELEASE_PLZ_YML.read_text(encoding="utf-8")

    def test_command_is_release_pr(self):
        self.assertIn("command: release-pr", self.text)
        self.assertNotIn("command: release\n", self.text)
        self.assertNotIn("command: release ", self.text)

    def test_no_publish_token_or_cargo_publish(self):
        self.assertNotIn("CARGO_REGISTRY_TOKEN", self.text)
        self.assertNotRegex(self.text, r"(?m)^\s*cargo\s+publish")


if __name__ == "__main__":
    unittest.main()
