import os, subprocess
runner='/workspace/work/goal-qualification-20261028/run-v2.py'
cases=[
('scratch',['test','--locked','--lib','oversized_reused_topic_name_is_discarded_for_a_small_next_response','--','--test-threads=1']),
('session-budget',['test','--locked','--lib','fetch_session_validation_rejects_v12_duplicates_and_bounds_topic_tracking','--','--test-threads=1']),
('session-broker',['test','--locked','--test','consumer_fetch_semantics','fetch_session_metadata_exhaustion_returns_without_retry','--','--exact','--test-threads=1']),
('network-unit',['test','--locked','--lib','net::tests::','--','--test-threads=1']),
('tls',['test','--locked','--test','tls_shutdown_deadline','--','--test-threads=1']),
('bootstrap',['test','--locked','--test','bootstrap_handshake','--','--test-threads=1']),
('init-producer',['test','--locked','--test','init_producer_id_v6','--','--test-threads=1']),
('network-deadlines',['test','--locked','--test','network_deadlines','--','--test-threads=1']),
('legacy-deadline',['test','--locked','--test','legacy_discovery_v0','producer_consumer_and_group_startup_keep_the_initial_deadline','--','--exact','--test-threads=1']),
('api-allocation',['test','--locked','--test','api_versions_allocation','--','--test-threads=1']),
('strict-lib-clippy',['clippy','--locked','--all-features','--lib','--','-D','warnings']),
]
for name,args in cases:
 env=os.environ.copy();env['QUALIFICATION_PROCESS_TIMEOUT_SECS']='300'
 print('START',name,flush=True)
 result=subprocess.run(['python3',runner,'client-final-focused-'+name+'-01',*args],env=env)
 print('END',name,'runner_exit',result.returncode,flush=True)
