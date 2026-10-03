def quarantine_retained_originals_after_compile():
    # Cargo may legitimately recreate the exact same source pathname with new
    # bytes. Original preservation depends on quarantine identities only.
    assert set(files(QUARANTINE))==set(INITIAL_PACKAGE_IDENTITIES) and len(INITIAL_PACKAGE_IDENTITIES)==25
    rows=[]
    for name,expected in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
        retained=require_quarantine_identity(QUARANTINE/name,expected)
        cache=observe_quarantine_identity(TARGET/name,expected)
        rows.append({'cache_relative_path':name,'quarantine_path':str(QUARANTINE/name),
                     'retained_original_identity':retained,'cache_path':str(TARGET/name),
                     'current_cache_path_diagnostic':cache,
                     'cache_path_role':'mutable compiler output; existence does not establish loss of retained original',
                     'current_cache_hash_scope':'compared to original only if regular and original byte length; library/ELF capture supplies actual generated artifact hashes separately'})
    return {'quarantine_original_file_count':25,'all25_original_sha_bytes_fullmodes_mtimes_inodes_preserved':True,
            'exact_quarantine25_pathset_verified':True,'source_path_absence_required_after_compile':False,
            'retained_originals_and_current_cache_paths':rows}
