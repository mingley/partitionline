import importlib.util, sys
spec = importlib.util.spec_from_file_location('qualification_runner', '/workspace/work/goal-qualification-20261028/run-v2.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.ENV['CARGO_TARGET_DIR'] = '/workspace/work/group-journal-target'
module.ENV['CARGO_NET_OFFLINE'] = 'true'
module.ROOTS.append('partitionline-broker/examples')
module.FILES.append('docs/evidence/broker/KL11-59/topic-name-cases.tsv')
sys.exit(module.run(sys.argv[1], ['cargo', '+stable', *sys.argv[2:]]))
