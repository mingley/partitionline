from pathlib import Path
import hashlib
import os

p = Path('/workspace/work/list-transactions-routing-01')
s = (p/'admin.before.rs').read_text()
begin = s.index('    /// [`Self::list_transactions_with_duration`] with a one-shot RPC')
end = s.index('    /// Describe ACL bindings', begin)
s = s[:begin]+(p/'methods.rs.part').read_text()+'\n'+s[end:]
s = s.replace('    list_transactions_version: Option<i16>,\n', '')
s = s.replace('            list_transactions_version,\n', '')
a = s.index('        let list_transactions_version = versions')
b = s.index('        let ', a+12)
s = s[:a]+s[b:]
s = s.replace('/// List transactional.id state (ListTransactions api 66).',
              '/// List transactional.id state from every broker (ListTransactions api 66).')
a = s.index('    /// Lands on the transaction coordinator (`FindCoordinator`\n',
            s.index('    /// List transactional.id state from'))
b = s.index('    /// Duration is unfiltered', a)
s = s[:a]+('    /// Queries every broker from Metadata and negotiates each connection.\n'
            '    /// Complete results require every broker to succeed; load delays retry the\n'
            '    /// affected broker under one caller deadline.\n')+s[b:]
s = s.replace('/// and the coordinator retry budget. DurationFilter stays `-1`',
              '/// and the total all-broker retry budget. DurationFilter stays `-1`')
a = s.index('pub(crate) async fn fetch_client_instance_id(')
s = s[:a]+(p/'support.rs.part').read_text()+'\n\n'+s[a:]
dst = p/'admin.candidate.rs'
if dst.exists() and dst.read_text() != s:
    h = hashlib.sha256(dst.read_bytes()).hexdigest()
    old = p/('admin.draft-'+h+'.rs')
    if not old.exists():
        old.write_bytes(dst.read_bytes())
        os.chmod(old, 0o600)
dst.write_text(s)
os.chmod(dst, 0o600)
print(hashlib.sha256(dst.read_bytes()).hexdigest())
