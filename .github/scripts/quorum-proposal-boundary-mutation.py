#!/usr/bin/env python3
"""Insert a diagnostic-only NotLeader after a real synthetic Node append."""
from pathlib import Path

path = Path("partitionline-broker/src/raft/replication.rs")
source = path.read_text()
before = """        self.storage_result(result)?;
        self.sync_summary()?;
        self.advance_commit()?;
        Ok(index)
    }
    fn peer_position"""
after = """        self.storage_result(result)?;
        if payload == b"PL_SYNTHETIC_PROPOSAL_BOUNDARY" {
            return Err(Error::NotLeader);
        }
        self.sync_summary()?;
        self.advance_commit()?;
        Ok(index)
    }
    fn peer_position"""
if source.count(before) != 1:
    raise SystemExit("unexpected Node::propose hunk")
path.write_text(source.replace(before, after))
print("installed synthetic NotLeader-after-append mutation")
