from pathlib import Path
import hashlib,json,stat,sys
work=Path('/workspace/work/client-share-assessment');source=work/'preparation-msrv-lint-96211da1';base=json.loads((work/'final-96211da1/source-verification.json').read_text());expected=json.loads((work/'runtime-msrv-lint-correction-final-variants.json').read_text())
for name,row in base['files'].items():
    path=source/name;mode=row['mode'];info=path.lstat()
    assert stat.S_ISLNK(info.st_mode)==(mode=='120000'),name
    if mode!='120000':assert stat.S_ISREG(info.st_mode) and bool(info.st_mode & 0o111)==(mode=='100755'),name
    data=path.readlink().as_posix().encode() if mode=='120000' else path.read_bytes()
    assert hashlib.sha256(data).hexdigest()==expected.get(name,row['sha256']),name
assert hashlib.sha256((source/'clippy.toml').read_bytes()).hexdigest()=='7ad5c55d408b8e15adb48a7054705ef3828ce59ff464a4a62a9842c88788fba6'
Path(sys.argv[1]).write_text(json.dumps({'phase':sys.argv[2],'base_source_sha':base['source_sha'],'candidate_source':'isolated preparation only; not a pushed source and not final qualification','verified_git_files_with_two_declared_variants':len(base['files']),'only_changed_source_sha256':expected,'clippy_toml_sha256':'7ad5c55d408b8e15adb48a7054705ef3828ce59ff464a4a62a9842c88788fba6'},indent=2)+'\n')
print('PASS isolated candidate44092basefiles/modes withonly2declaredcodevariants')
