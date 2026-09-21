"""
Unit tests for the release CI eligibility gate (scripts/check-main-ci.sh).

This test suite satisfies card KL08-01 (audit A12 repair):
- Rejects non-CI workflows (such as Release-plz, release, first-publish) as substitutes
  for the required CI workflow ('ci' / 'ci.yml').
- Validates exact workflow identity and exact commit SHA.
- Distinguishes wrong SHA, missing, pending, cancelled, and failed CI as distinct outcomes.
- Uses read-only, offline fixtures without querying or mutating real workflow state.

Outcome Mapping:
  Outcome       Exit (REQUIRE=0)  Exit (REQUIRE=1)  Signature in Stderr / Stdout
  -----------   ----------------  ----------------  -------------------------------------------------------------
  success       0                 0                 stdout: [outcome=success] (main HEAD CI is green)
  failed        1                 1                 stderr: [outcome=failed] (failed: ...)
  cancelled     1                 1                 stderr: [outcome=cancelled] (cancelled: ...)
  pending       2                 1                 stderr: [outcome=pending] (pending: CI run ... still running/queued)
  missing       2                 1                 stderr: [outcome=missing] (missing: no CI workflow run found ...)
  wrong_sha     2                 1                 stderr: [outcome=wrong_sha] (wrong SHA: ...)

Contract & Gate Guarantees:
  - Exit code 0 is returned ONLY when the exact CI workflow for the exact SHA completed with success.
  - When REQUIRE_MAIN_CI=1, any non-success (failed, cancelled, pending, missing, wrong_sha) exits 1.
  - When REQUIRE_MAIN_CI=0, terminal failures (failed, cancelled) exit 1, while inconclusive states
    (pending, missing, wrong_sha) exit 2.
  - Non-CI workflows never satisfy the gate, even if completed with success.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check-main-ci.sh"

TARGET_SHA = "1111111111111111111111111111111111111111"
OTHER_SHA = "2222222222222222222222222222222222222222"


def run_gate(
    runs_data,
    sha: str = TARGET_SHA,
    require_main_ci: str = "1",
    extra_env: dict = None,
    runs_paths: list = None,
):
    """
    Run scripts/check-main-ci.sh with fixture data passed via GH_RUNS_JSON.
    Does not require git remote or network access.
    """
    env = dict(os.environ)
    env["CHECK_SHA"] = sha
    env["REQUIRE_MAIN_CI"] = require_main_ci
    env["MAIN_BRANCH"] = "main"

    if extra_env:
        env.update(extra_env)

    temp_files = []
    try:
        if runs_paths is not None:
            fixture_spec = ":".join(str(p) for p in runs_paths)
        else:
            tf = tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".json", delete=False
            )
            temp_files.append(tf.name)
            json.dump(runs_data, tf)
            tf.close()
            fixture_spec = tf.name

        env["GH_RUNS_JSON"] = fixture_spec

        result = subprocess.run(
            ["bash", str(SCRIPT_PATH)],
            env=env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        return result
    finally:
        for p in temp_files:
            try:
                os.remove(p)
            except OSError:
                pass


class TestReleaseCIGate(unittest.TestCase):
    """Test suite for release CI eligibility gate."""

    def test_green_release_plz_without_ci_fails_when_require_main_ci_1(self):
        """
        Audit A12 primary deliverable:
        A green Release-plz run with no CI run fails when REQUIRE_MAIN_CI=1.
        """
        runs = [
            {
                "databaseId": 50001,
                "name": "Release-plz",
                "workflowName": "Release-plz",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res.returncode, 1)
        self.assertIn("[outcome=missing]", res.stderr)
        self.assertIn("missing: no CI workflow run found", res.stderr)
        self.assertIn("Release-plz", res.stderr)

    def test_green_release_plz_without_ci_inconclusive_when_require_main_ci_0(self):
        """
        With REQUIRE_MAIN_CI=0, missing CI is inconclusive (exit 2).
        """
        runs = [
            {
                "databaseId": 50001,
                "name": "Release-plz",
                "workflowName": "Release-plz",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res.returncode, 2)
        self.assertIn("[outcome=missing]", res.stderr)

    def test_wrong_sha_runs_for_other_commits_only(self):
        """
        Runs exist in the listing, but none match the requested SHA.
        Outcome is distinct: wrong_sha.
        """
        runs = [
            {
                "databaseId": 50002,
                "name": "ci",
                "workflowName": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": OTHER_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        # With REQUIRE_MAIN_CI=1
        res1 = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=wrong_sha]", res1.stderr)
        self.assertIn("wrong SHA", res1.stderr)

        # With REQUIRE_MAIN_CI=0
        res0 = run_gate(runs, sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res0.returncode, 2)
        self.assertIn("[outcome=wrong_sha]", res0.stderr)

    def test_wrong_sha_unresolvable_ref(self):
        """
        Unresolvable git reference / invalid SHA string produces wrong_sha outcome.
        """
        res = run_gate([], sha="invalid!nonexistent!sha", require_main_ci="1")
        self.assertEqual(res.returncode, 1)
        self.assertIn("[outcome=wrong_sha]", res.stderr)
        self.assertIn("cannot resolve", res.stderr)

    def test_missing_runs_empty_list(self):
        """
        Empty run list produces missing outcome.
        """
        res1 = run_gate([], sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=missing]", res1.stderr)

        res0 = run_gate([], sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res0.returncode, 2)
        self.assertIn("[outcome=missing]", res0.stderr)

    def test_pending_ci_in_progress(self):
        """
        CI run for exact SHA is in_progress (pending outcome).
        """
        runs = [
            {
                "databaseId": 50003,
                "name": "ci",
                "workflowName": "ci",
                "status": "in_progress",
                "conclusion": None,
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res1 = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=pending]", res1.stderr)

        res0 = run_gate(runs, sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res0.returncode, 2)
        self.assertIn("[outcome=pending]", res0.stderr)

    def test_pending_ci_queued(self):
        """
        CI run for exact SHA is queued (pending outcome).
        """
        runs = [
            {
                "databaseId": 50004,
                "name": "ci",
                "workflowName": "ci",
                "status": "queued",
                "conclusion": None,
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res1 = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=pending]", res1.stderr)

    def test_cancelled_ci(self):
        """
        CI run for exact SHA was cancelled (cancelled outcome).
        Cancelled CI is a non-success terminal state and exits 1 under both settings.
        """
        runs = [
            {
                "databaseId": 50005,
                "name": "ci",
                "workflowName": "ci",
                "status": "completed",
                "conclusion": "cancelled",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res1 = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=cancelled]", res1.stderr)

        res0 = run_gate(runs, sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res0.returncode, 1)
        self.assertIn("[outcome=cancelled]", res0.stderr)

    def test_failed_ci(self):
        """
        CI run for exact SHA completed with failure (failed outcome).
        Exits 1 under both REQUIRE_MAIN_CI settings.
        """
        runs = [
            {
                "databaseId": 50006,
                "name": "ci",
                "workflowName": "ci",
                "status": "completed",
                "conclusion": "failure",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res1 = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res1.returncode, 1)
        self.assertIn("[outcome=failed]", res1.stderr)

        res0 = run_gate(runs, sha=TARGET_SHA, require_main_ci="0")
        self.assertEqual(res0.returncode, 1)
        self.assertIn("[outcome=failed]", res0.stderr)

    def test_successful_ci(self):
        """
        Exact CI workflow run completed with success: exit code 0.
        """
        runs = [
            {
                "databaseId": 50007,
                "name": "ci",
                "workflowName": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
                "attempt": 1,
            }
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res.returncode, 0)
        self.assertIn("[outcome=success]", res.stdout)
        self.assertIn("main HEAD CI is green", res.stdout)

    def test_attempt_selection_selects_latest_success_after_failure(self):
        """
        When attempt 1 failed and attempt 2 succeeded, the gate selects attempt 2.
        Order in the fixture must not break attempt resolution.
        """
        # Fixture with attempt 1 first
        runs1 = [
            {
                "databaseId": 60001,
                "name": "ci",
                "status": "completed",
                "conclusion": "failure",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:00:00Z",
                "attempt": 1,
            },
            {
                "databaseId": 60001,
                "name": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:15:00Z",
                "attempt": 2,
            },
        ]
        res1 = run_gate(runs1, sha=TARGET_SHA)
        self.assertEqual(res1.returncode, 0)
        self.assertIn("[outcome=success]", res1.stdout)
        self.assertIn("attempt 2", res1.stdout)

        # Fixture with attempt 2 first
        runs2 = list(reversed(runs1))
        res2 = run_gate(runs2, sha=TARGET_SHA)
        self.assertEqual(res2.returncode, 0)
        self.assertIn("[outcome=success]", res2.stdout)
        self.assertIn("attempt 2", res2.stdout)

    def test_attempt_selection_selects_latest_running_after_success(self):
        """
        If attempt 1 succeeded but a re-run (attempt 2) is currently in_progress,
        the gate selects attempt 2 and reports pending (does NOT greenwash on old attempt).
        """
        runs = [
            {
                "databaseId": 60002,
                "name": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:00:00Z",
                "attempt": 1,
            },
            {
                "databaseId": 60002,
                "name": "ci",
                "status": "in_progress",
                "conclusion": None,
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:30:00Z",
                "attempt": 2,
            },
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res.returncode, 1)
        self.assertIn("[outcome=pending]", res.stderr)
        self.assertIn("attempt 2", res.stdout)

    def test_attempt_selection_selects_latest_failure_after_success(self):
        """
        If attempt 1 succeeded but a re-run (attempt 2) failed,
        the gate selects attempt 2 and reports failure.
        """
        runs = [
            {
                "databaseId": 60003,
                "name": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:00:00Z",
                "attempt": 1,
            },
            {
                "databaseId": 60003,
                "name": "ci",
                "status": "completed",
                "conclusion": "failure",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T10:30:00Z",
                "attempt": 2,
            },
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res.returncode, 1)
        self.assertIn("[outcome=failed]", res.stderr)
        self.assertIn("attempt 2", res.stdout)

    def test_pagination_matching_ci_on_second_page(self):
        """
        Test multi-page fixture: page 1 contains 30 runs of other workflows/commits,
        page 2 contains the successful CI run for the target SHA.
        The gate must traverse pages and locate the CI run on page 2.
        """
        page1 = [
            {
                "databaseId": 70000 + i,
                "name": "Release-plz" if i % 2 == 0 else "dependabot",
                "status": "completed",
                "conclusion": "success",
                "headSha": OTHER_SHA,
                "createdAt": f"2026-09-21T12:{i:02d}:00Z",
                "attempt": 1,
            }
            for i in range(30)
        ]
        page2 = [
            {
                "databaseId": 70100,
                "name": "ci",
                "workflowName": "ci",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T11:00:00Z",
                "attempt": 1,
            }
        ]

        # Multi-page fixture format 1: list of pages [page1, page2]
        res1 = run_gate([page1, page2], sha=TARGET_SHA)
        self.assertEqual(res1.returncode, 0)
        self.assertIn("[outcome=success]", res1.stdout)
        self.assertIn("run 70100", res1.stdout)

        # Multi-page fixture format 2: dict with "pages" key
        res2 = run_gate({"pages": [page1, page2]}, sha=TARGET_SHA)
        self.assertEqual(res2.returncode, 0)
        self.assertIn("[outcome=success]", res2.stdout)

    def test_pagination_with_colon_separated_files(self):
        """
        Test pagination using colon-separated file paths passed to GH_RUNS_JSON.
        """
        tf1 = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".json", delete=False
        )
        tf2 = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".json", delete=False
        )
        try:
            # File 1 has non-matching runs
            json.dump(
                [
                    {
                        "databaseId": 80001,
                        "name": "Release-plz",
                        "status": "completed",
                        "conclusion": "success",
                        "headSha": OTHER_SHA,
                        "createdAt": "2026-09-21T12:00:00Z",
                    }
                ],
                tf1,
            )
            tf1.close()

            # File 2 has matching CI run
            json.dump(
                [
                    {
                        "databaseId": 80002,
                        "name": "ci",
                        "status": "completed",
                        "conclusion": "success",
                        "headSha": TARGET_SHA,
                        "createdAt": "2026-09-21T11:00:00Z",
                    }
                ],
                tf2,
            )
            tf2.close()

            res = run_gate(
                None,
                sha=TARGET_SHA,
                require_main_ci="1",
                runs_paths=[tf1.name, tf2.name],
            )
            self.assertEqual(res.returncode, 0)
            self.assertIn("[outcome=success]", res.stdout)
            self.assertIn("run 80002", res.stdout)
        finally:
            for p in (tf1.name, tf2.name):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def test_non_ci_workflows_never_substitute_for_ci(self):
        """
        Verify that various green non-CI workflows (Release-plz, release, first-publish)
        never satisfy the check when the CI workflow failed.
        """
        runs = [
            {
                "databaseId": 90001,
                "name": "Release-plz",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:00:00Z",
            },
            {
                "databaseId": 90002,
                "name": "release",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:01:00Z",
            },
            {
                "databaseId": 90003,
                "name": "first-publish",
                "status": "completed",
                "conclusion": "success",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:02:00Z",
            },
            {
                "databaseId": 90004,
                "name": "ci",
                "status": "completed",
                "conclusion": "failure",
                "headSha": TARGET_SHA,
                "createdAt": "2026-09-21T12:03:00Z",
            },
        ]
        res = run_gate(runs, sha=TARGET_SHA, require_main_ci="1")
        self.assertEqual(res.returncode, 1)
        self.assertIn("[outcome=failed]", res.stderr)
        self.assertIn("run 90004", res.stdout)

    def test_distinct_outcomes_are_not_collapsed(self):
        """
        Verify that all non-success outcomes (wrong_sha, missing, pending, cancelled, failed)
        produce distinct signature tokens and messages.
        """
        signatures = {
            "wrong_sha": run_gate(
                [{"headSha": OTHER_SHA, "name": "ci"}], sha=TARGET_SHA
            ).stderr,
            "missing": run_gate(
                [{"headSha": TARGET_SHA, "name": "Release-plz"}], sha=TARGET_SHA
            ).stderr,
            "pending": run_gate(
                [{"headSha": TARGET_SHA, "name": "ci", "status": "in_progress"}],
                sha=TARGET_SHA,
            ).stderr,
            "cancelled": run_gate(
                [
                    {
                        "headSha": TARGET_SHA,
                        "name": "ci",
                        "status": "completed",
                        "conclusion": "cancelled",
                    }
                ],
                sha=TARGET_SHA,
            ).stderr,
            "failed": run_gate(
                [
                    {
                        "headSha": TARGET_SHA,
                        "name": "ci",
                        "status": "completed",
                        "conclusion": "failure",
                    }
                ],
                sha=TARGET_SHA,
            ).stderr,
        }

        tokens = set()
        for outcome, stderr_text in signatures.items():
            expected_token = f"[outcome={outcome}]"
            self.assertIn(
                expected_token,
                stderr_text,
                f"Expected {expected_token} in stderr for {outcome}",
            )
            tokens.add(expected_token)

        # Ensure all 5 signatures are unique
        self.assertEqual(len(tokens), 5)


if __name__ == "__main__":
    unittest.main()
