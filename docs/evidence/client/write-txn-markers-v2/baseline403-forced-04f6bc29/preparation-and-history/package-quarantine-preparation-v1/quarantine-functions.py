def observe_quarantine_identity(path,expected):
    # Diagnostic inspection remains bounded by each reviewed artifact's length.
    if not os.path.lexists(path):return {'present':False}
    try:
        st=path.lstat()
        result={'present':True,'regular':stat.S_ISREG(st.st_mode),'bytes':st.st_size,
                'full_mode':stat.S_IMODE(st.st_mode),'mtime_ns':st.st_mtime_ns,
                'inode':[st.st_dev,st.st_ino]}
        if result['regular'] and st.st_size==expected['bytes']:
            result['sha256']=digest(path.read_bytes())
        result['identity_matches_expected']=all(result.get(k)==expected[k] for k in ['bytes','full_mode','mtime_ns','inode','sha256']) and result['regular']
        return result
    except BaseException as fault:
        return {'present':True,'inspection_error':type(fault).__name__,'identity_matches_expected':False}

def require_quarantine_identity(path,expected):
    result=observe_quarantine_identity(path,expected)
    assert result.get('identity_matches_expected'), ('reviewed package identity changed',str(path),result)
    return result

def quarantine_location_snapshot(moved):
    rows=[]
    for name,expected in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
        source=observe_quarantine_identity(TARGET/name,expected)
        retained=observe_quarantine_identity(QUARANTINE/name,expected)
        exactly_once=source.get('present',False)!=retained.get('present',False)
        intact=exactly_once and (source if source.get('present') else retained).get('identity_matches_expected',False)
        rows.append({'cache_relative_path':name,'source_path':str(TARGET/name),
                     'quarantine_path':str(QUARANTINE/name),'source':source,'quarantine':retained,
                     'exactly_once_and_intact':intact,'completed_in_ledger':name in moved})
    return {'observed_locations':rows,'all25_present_exactly_once_and_intact':len(rows)==25 and all(r['exactly_once_and_intact'] for r in rows),
            'completed_rename_count':len(moved),'automatic_rollback_or_retry':False}

def quarantine_log_event(event):
    # Fresh exclusive ledger; each planned and completed rename is flushed.
    path=RUN/'quarantine-moves.jsonl'
    with path.open('a') as output:
        output.write(json.dumps(event,sort_keys=True)+'\n');output.flush();os.fsync(output.fileno())

def quarantine_own_package_outputs(checked):
    expected=checked['complete_cache_identity_map']
    assert len(expected)==609 and set(expected)==set(FULL_CACHE_REFERENCE)|{'CACHEDIR.TAG'}
    verify_complete_cache_map(expected)
    targetst=TARGET.lstat()
    assert stat.S_ISDIR(targetst.st_mode) and targetst.st_uid==os.getuid() and stat.S_IMODE(targetst.st_mode)==0o700
    assert (targetst.st_dev,targetst.st_ino)==(27,524404), 'owned target identity changed'
    assert len(INITIAL_PACKAGE_IDENTITIES)==25
    for name,row in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and len(Path(name).parts)<=4
        assert name in expected and all(row[k]==expected[name][k] for k in ['sha256','bytes','full_mode','mtime_ns'])
        require_quarantine_identity(TARGET/name,row)
        compressed=Path(row['gzip_path']).read_bytes()
        assert digest(compressed)==row['gzip_sha256'] and gzip.decompress(compressed)==(TARGET/name).read_bytes()
    assert not os.path.lexists(QUARANTINE), 'never overwrite a prior quarantine directory'
    parent=QUARANTINE.parent.lstat()
    assert stat.S_ISDIR(parent.st_mode) and parent.st_uid==os.getuid() and parent.st_dev==targetst.st_dev
    # mkdir without exist_ok is exclusive. Trusted0700 destination plus the
    # exclusive lease prevents cooperating writers; no universal race-free claim.
    QUARANTINE.mkdir(mode=0o700)
    qst=QUARANTINE.lstat()
    assert stat.S_ISDIR(qst.st_mode) and stat.S_IMODE(qst.st_mode)==0o700 and qst.st_uid==os.getuid() and qst.st_dev==targetst.st_dev
    ledger=RUN/'quarantine-moves.jsonl'
    with ledger.open('x') as output:output.flush();os.fsync(output.fileno())
    ledger.chmod(0o600)
    moved=[]
    try:
        parents={str(Path(name).parent) for name in INITIAL_PACKAGE_IDENTITIES}
        assert len(parents)<=25
        for relative in sorted(parents):
            path=QUARANTINE
            for component in Path(relative).parts:
                path=path/component
                if not os.path.lexists(path):path.mkdir(mode=0o700)
                st=path.lstat()
                assert stat.S_ISDIR(st.st_mode) and st.st_uid==os.getuid() and stat.S_IMODE(st.st_mode)==0o700 and st.st_dev==targetst.st_dev
        for name,row in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
            if INTERRUPTED is not None:raise InterruptedError('controlled before-rename interruption')
            assert set(files(TARGET))==set(expected)-set(moved), 'cache pathset changed during quarantine'
            source=TARGET/name;dest=QUARANTINE/name
            require_quarantine_identity(source,row)
            path=QUARANTINE
            for component in Path(name).parts[:-1]:
                path=path/component;st=path.lstat()
                assert stat.S_ISDIR(st.st_mode) and stat.S_IMODE(st.st_mode)==0o700 and st.st_uid==os.getuid() and st.st_dev==targetst.st_dev
            assert not os.path.lexists(dest), ('existing quarantine destination',str(dest))
            quarantine_log_event({'operation':'rename_planned','cache_relative_path':name,
                'source_path':str(source),'quarantine_path':str(dest),
                'expected_identity':{k:row[k] for k in ['sha256','bytes','full_mode','mtime_ns','inode']}})
            os.rename(source,dest)
            assert not os.path.lexists(source), ('source path remained after rename',str(source))
            actual=require_quarantine_identity(dest,row)
            moved.append(name)
            quarantine_log_event({'operation':'rename_completed','cache_relative_path':name,'actual_identity':actual})
        after=files(TARGET)
        assert set(after)==set(expected)-set(INITIAL_PACKAGE_IDENTITIES) and len(after)==584
        survivors={name:row for name,row in expected.items() if name not in INITIAL_PACKAGE_IDENTITIES}
        verify_complete_cache_map(survivors)
        assert set(files(QUARANTINE))==set(INITIAL_PACKAGE_IDENTITIES), 'quarantine regular pathset changed'
        locations=quarantine_location_snapshot(moved)
        assert locations['all25_present_exactly_once_and_intact']
        result={'schema_version':1,'kind':'ROOT-authorized-reversible-exact25-owned-package-quarantine',
            'source_sha':PLAN['source_sha'],'quarantine_directory':str(QUARANTINE),
            'quarantine_directory_identity':{'inode':[qst.st_dev,qst.st_ino],'uid':qst.st_uid,'full_mode':stat.S_IMODE(qst.st_mode)},
            'original_cache_files':609,'retained_survivor_files':584,'quarantined_original_files':25,
            'all584_survivor_sha_bytes_fullmodes_mtimes_unchanged':True,
            'all25_original_sha_bytes_fullmodes_mtimes_inodes_preserved':True,
            'all25_existing_gzip_decompression_verified':True,'retained_locations':locations,
            'actual_reclaimed_file_bytes':0,'raw_original_bytes_deleted_copied_or_reencoded':False,
            'standard_CACHEDIR_TAG_left_unchanged':True,'actual_Cargo_clean_or_dryrun_commands':0,
            'rename_ledger_sha256':digest(ledger.read_bytes()),'automatic_rollback_or_retry':False,
            'universal_race_free_ownership_or_power_loss_durability_claim':False}
        output=RUN/'package-quarantine-receipt.json';output.write_text(json.dumps(result,indent=2)+'\n');output.chmod(0o600)
        return result
    except BaseException as fault:
        # Preserve every surviving source/destination and the actual planned vs
        # completed ledger. No delete, chmod of originals, rollback or retry.
        result={'schema_version':1,'kind':'actual-partial-quarantine-failure','error_type':type(fault).__name__,
            'quarantine_directory':str(QUARANTINE),'locations':quarantine_location_snapshot(moved),
            'rename_ledger_sha256':digest(ledger.read_bytes()),'automatic_rollback_or_retry':False,
            'following_compiler_or_test_launch_permitted':False}
        output=RUN/'package-quarantine-failure.json';output.write_text(json.dumps(result,indent=2)+'\n');output.chmod(0o600)
        raise
