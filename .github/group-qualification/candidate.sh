#!/usr/bin/env bash
set -euo pipefail

export CARGO_TARGET_DIR="$GITHUB_WORKSPACE/target"
tests=(
  'net::tests::physical_retirement_timeout_keeps_a_closed_retryable_slot'
  'net::tests::cancelling_physical_retirement_keeps_a_closed_retryable_slot'
  'net::tests::repeated_physical_retirement_outside_runtime_needs_no_cleanup_task'
  'net::tests::canceled_queued_stream_waiter_finishes_retirement'
  'net::tests::queued_stream_guard_handoff_finishes_retirement'
  'net::tests::dropped_watcher_waiter_retires_a_reserved_stream'
  'net::tests::canceled_retirement_waiter_closes_after_watcher_drops'
  'net::tests::dropping_owner_retires_a_stream_locked_by_its_watcher'
  'net::tests::runtime_shutdown_retires_a_stream_locked_by_its_watcher'
  'net::tests::retirement_wakes_a_watcher_already_draining_a_closed_socket'
  'group::coordinator_replacement_tests::canceled_versions_retires_both_sockets_and_can_retry'
  'group::coordinator_replacement_tests::canceled_sasl_restores_versions_and_retires_replacement'
  'group::coordinator_replacement_tests::expired_replacement_retires_old_socket_without_dialing'
  'group::coordinator_replacement_tests::stalled_replacement_obeys_deadline_and_retires_socket'
  'group::rediscover_stop_tests::replacing_an_idle_coordinator_lets_the_broker_leave'
  'group::rediscover_stop_tests::replacing_an_idle_offset_coordinator_lets_the_broker_leave'
  'group::rediscover_stop_tests::replacing_an_idle_legacy_offset_coordinator_lets_the_broker_leave'
  'group::offsets::reconnect_tests::idle_offset_replacement_authenticates_and_reencodes_for_the_new_version'
  'group::offsets::reconnect_tests::idle_offset_replacement_never_downgrades_uuid_requests'
  'group::offsets::reconnect_tests::failed_offset_request_retires_socket_and_reencodes_for_new_version'
  'group::offsets::reconnect_tests::typed_idle_offset_replacement_uses_the_new_authenticated_version'
  'group::offsets::reconnect_tests::offset_load_retry_rediscovers_after_second_coordinator_error'
  'group::offsets::reconnect_tests::v10_load_retry_never_downgrades_uuid_requests'
)

for feature in default all; do
  for test_name in "${tests[@]}"; do
    log="$(mktemp)"
    args=(+stable test --locked --lib "$test_name")
    if [ "$feature" = all ]; then
      args+=(--all-features)
    fi
    args+=(-- --exact --nocapture)
    status=0
    cargo "${args[@]}" >"$log" 2>&1 || status=$?
    cat "$log"
    test "$status" -eq 0
    grep -Fq 'test result: ok. 1 passed; 0 failed;' "$log"
    test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
    test -n "$test_bin"
    test_bin="$(realpath "$test_bin")"
    case "$test_bin" in
      "$GITHUB_WORKSPACE"/target/*) ;;
      *) printf 'unexpected candidate executable: %s\n' "$test_bin" >&2; exit 1 ;;
    esac
    if [ "$feature" = default ]; then
      sha256sum "$test_bin" | cut -d ' ' -f 1 > "$GITHUB_WORKSPACE/candidate-test.sha256"
    fi
    printf 'candidate executable: %s\n' "$test_bin"
    rm "$log"
  done
done

for feature in default all; do
  log="$(mktemp)"
  args=(+stable test --locked --test group_coord_tls_retirement idle_group_coordinator_retires_previous_tls_peer)
  if [ "$feature" = all ]; then
    args+=(--all-features)
  fi
  args+=(-- --exact --nocapture)
  status=0
  cargo "${args[@]}" >"$log" 2>&1 || status=$?
  cat "$log"
  test "$status" -eq 0
  grep -Fq 'test result: ok. 1 passed; 0 failed;' "$log"
  rm "$log"
done
