"""Incomplete, corrupted or substituted broker cells cannot qualify."""
import contextlib
import io
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('broker_report', ROOT / 'scripts/report-broker-compatibility.py')
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)
CELL = json.loads((ROOT / 'tests/conformance/current-broker-cells.json').read_text())['cells'][0]
SOURCE = '1' * 40


def fixture():
    prefix = 'plcompat-' + CELL['version'].replace('.', '-')
    identity = {'requested': CELL['reference'], 'actual_reference': CELL['reference'],
        'repo_digests': ['apache/kafka@' + CELL['digest']],
        'container_image_id': 'sha256:' + '2' * 64, 'inspected_image_id': 'sha256:' + '2' * 64,
        'kafka_cli_version': CELL['version'] + ' (Commit:fixture)',
        'host_os':'Linux', 'host_arch':'x86_64', 'rustc_host':'x86_64-unknown-linux-gnu', 'rustc_release':'1.98.1'}
    result = {'source_sha': SOURCE, 'requested': CELL['reference'], 'input_topic':prefix+'-input',
        'output_topic':prefix+'-output','seed_timestamp':1000000,
        'scenarios': {name: {'status':'passed','records':16,'duplicates':0,'missing':0,'corrupt':0}
                      for name in CELL['required_scenarios']},
        'api_ranges': {str(key):[0,20] for key in [0,1,2,11,68,76,78,79]},
        'finalized_features': {name:[1,1] for name in ['share.version','group.version','transaction.version','metadata.version']},
        'committed_offsets': {name:{'0':8,'1':8} for name in ['classic','cooperative','kip848','transaction']},
        'group_ids': {name:prefix+'-'+name for name in ['classic','cooperative','kip848','transaction']},
        'transaction_aborted_visible':0,'share_accepted':16,'startup_attempts':[]}
    return identity, result


def runtime_text(result):
    lines = ['PL_COMPAT_SOURCE\t'+result['source_sha'], 'PL_COMPAT_REFERENCE\t'+result['requested'],
             'PL_COMPAT_INPUT_TOPIC\t'+result['input_topic'], 'PL_COMPAT_OUTPUT_TOPIC\t'+result['output_topic'],
             'PL_COMPAT_SEED_TIMESTAMP\t'+str(result['seed_timestamp'])]
    for name, case in result['scenarios'].items():
        lines.append('\t'.join(['PL_COMPAT_SCENARIO',name,case['status']]+[str(case[k]) for k in ['records','duplicates','missing','corrupt']]))
    for tag, field in [('PL_COMPAT_API','api_ranges'),('PL_COMPAT_FEATURE','finalized_features')]:
        lines += ['\t'.join([tag,name,*map(str,values)]) for name,values in result[field].items()]
    for name, offsets in result['committed_offsets'].items():
        lines += ['\t'.join(['PL_COMPAT_COMMITTED',name,p,str(offset)]) for p,offset in offsets.items()]
    lines += ['\t'.join(['PL_COMPAT_GROUP',name,id]) for name,id in result['group_ids'].items()]
    lines += ['PL_COMPAT_ABORTED_VISIBLE\t0','PL_COMPAT_SHARE_ACCEPTED\t16','PL_COMPAT_COMPLETE',
              'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1s']
    return '\n'.join(lines)+'\n'


def java_files(directory, result):
    for name in ('input','output'):
        rows = []
        for partition in range(2):
            for index in range(8):
                target_partition = partition if name == 'input' else 0
                offset = index if name == 'input' else partition*8+index
                rows.append(f'CreateTime:{result["seed_timestamp"]+index}\tPartition:{target_partition}\tOffset:{offset}\t{partition}:{index}\tKL01-10/{partition}/{index}/'+ 'x'*(index+1))
        (directory/f'java-{name}.log').write_text('\n'.join(rows)+'\n')
    for group,id in result['group_ids'].items():
        (directory/f'java-offsets-{group}.log').write_text('\n'.join(f'{id} {result["input_topic"]} {p} 8 8 0 - - -' for p in range(2))+'\n')
    (directory/'features.log').write_text('\n'.join(f'Feature: {name}\tSupportedMinVersion: 0\tSupportedMaxVersion: 1\tFinalizedVersionLevel: 1' for name in result['finalized_features'])+'\n')


class BrokerCompatibility(unittest.TestCase):
    def test_complete_exact_profile_and_java_qualifies(self):
        identity,result = fixture()
        report.validate(identity,result,CELL,SOURCE)
        self.assertEqual(report.parse_runtime(runtime_text(result)),result)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            (directory/'identity.json').write_text(json.dumps(identity))
            (directory/'runtime.log').write_text(runtime_text(result))
            java_files(directory,result)
            with contextlib.redirect_stdout(io.StringIO()):
                report.finish(directory,CELL['version'],SOURCE)
            self.assertEqual(json.loads((directory/'report.json').read_text())['status'],'passed')

    def test_substitution_or_incomplete_scenarios_fail(self):
        edits = [lambda i,r:i.update(actual_reference='external:localhost:9092'),
            lambda i,r:i.update(repo_digests=[]), lambda i,r:i.update(container_image_id='sha256:'+'3'*64),
            lambda i,r:i.update(kafka_cli_version='4.1.0'), lambda i,r:i.update(host_arch='aarch64'),
            lambda i,r:r.update(source_sha='4'*40), lambda i,r:r['scenarios'].pop('share'),
            lambda i,r:r['scenarios']['kip848'].update(status='unsupported'),
            lambda i,r:r['scenarios']['manual'].update(records=0),
            lambda i,r:r['scenarios']['manual'].update(corrupt=False),
            lambda i,r:r['api_ranges'].pop('68'), lambda i,r:r['finalized_features']['share.version'].__setitem__(0,0),
            lambda i,r:r['committed_offsets']['transaction'].update({'0':7}),
            lambda i,r:r.update(transaction_aborted_visible=1),lambda i,r:r.update(share_accepted=15),
            lambda i,r:r['group_ids'].pop('cooperative')]
        for edit in edits:
            with self.subTest(edit=edits.index(edit)):
                identity,result=fixture(); edit(identity,result)
                with self.assertRaises((ValueError,KeyError)):
                    report.validate(identity,result,CELL,SOURCE)

    def test_missing_or_duplicate_completion_fields_fail(self):
        _,result=fixture(); output=runtime_text(result)
        for bad in [output.replace('PL_COMPAT_COMPLETE\n',''),output+'PL_COMPAT_COMPLETE\n',
                    output+'PL_COMPAT_SCENARIO\n', output+'PL_COMPAT_SOURCE\t'+SOURCE+'\n',
                    output+'PL_COMPAT_COMMITTED\tclassic\t0\t8\n',output+'PL_COMPAT_UNKNOWN\tx\n']:
            with self.assertRaises(ValueError): report.parse_runtime(bad)

    def test_independent_java_corruption_missing_offsets_and_feature_disagreement_fail(self):
        _,result=fixture()
        for filename, change in [('java-input.log',lambda s:s.replace('KL01-10/0/0/x','corrupt')),
            ('java-output.log',lambda s:s.splitlines()[0]+'\n'+s),
            ('java-input.log',lambda s:'\n'.join(s.splitlines()[1:])+'\n'),
            ('java-offsets-kip848.log',lambda s:s.replace(' 8 8 0 ',' 7 8 1 ')),
            ('features.log',lambda s:s.replace('FinalizedVersionLevel: 1','FinalizedVersionLevel: 0'))]:
            with tempfile.TemporaryDirectory() as temporary:
                directory=Path(temporary);java_files(directory,result)
                path=directory/filename;path.write_text(change(path.read_text()))
                with self.assertRaises(ValueError):report.validate_java(directory,result,CELL)


if __name__=='__main__': unittest.main()
