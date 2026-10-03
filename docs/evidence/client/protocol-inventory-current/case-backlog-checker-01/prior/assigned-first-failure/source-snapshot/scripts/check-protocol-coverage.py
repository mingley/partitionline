#!/usr/bin/env python3
"""
Protocol Coverage and Drift Checker (KL01-11).

Performs a deterministic comparison of the pinned Apache Kafka schema/API inventory
with partitionline codec, runtime, and conformance test coverage.

Enforces:
  1. Do not count a key name or helper type as an implemented client operation.
  2. Report version gaps, missing runtime wiring, and excluded broker-internal APIs separately.
  3. One synthetic new API or version fails the check closed until classified.
  4. Deterministic reporting against frozen Apache pins (3.9.1, 4.1.0, 4.1.2, 4.2.1, 4.3.1).
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


FROZEN_PINS = ["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"]

# Pinned Apache API Key inventory across Kafka 3.9.1 through 4.3.1.
# Format: api_key -> { "name": str, "pinned_versions": { pin: [min_v, max_v] } }
# Derived from pinned Apache message JSON schemas and ApiKeys.java.
HISTORICAL_APACHE_APIS: Dict[int, Dict[str, Any]] = {
    0: {"name": "Produce", "versions": {"3.9.1": [0, 11], "4.1.0": [3, 13], "4.1.2": [3, 13], "4.2.1": [3, 13], "4.3.1": [3, 13]}},
    1: {"name": "Fetch", "versions": {"3.9.1": [0, 17], "4.1.0": [4, 18], "4.1.2": [4, 18], "4.2.1": [4, 18], "4.3.1": [4, 18]}},
    2: {"name": "ListOffsets", "versions": {"3.9.1": [0, 9], "4.1.0": [1, 10], "4.1.2": [1, 10], "4.2.1": [1, 10], "4.3.1": [1, 11]}},
    3: {"name": "Metadata", "versions": {"3.9.1": [0, 12], "4.1.0": [0, 13], "4.1.2": [0, 13], "4.2.1": [0, 13], "4.3.1": [0, 13]}},
    4: {"name": "LeaderAndIsr", "versions": {"3.9.1": [0, 7], "4.1.0": [0, 7], "4.1.2": [0, 7], "4.2.1": [0, 7], "4.3.1": [0, 7]}},
    5: {"name": "StopReplica", "versions": {"3.9.1": [0, 4], "4.1.0": [0, 4], "4.1.2": [0, 4], "4.2.1": [0, 4], "4.3.1": [0, 4]}},
    6: {"name": "UpdateMetadata", "versions": {"3.9.1": [0, 8], "4.1.0": [0, 8], "4.1.2": [0, 8], "4.2.1": [0, 8], "4.3.1": [0, 8]}},
    7: {"name": "ControlledShutdown", "versions": {"3.9.1": [0, 3], "4.1.0": [0, 3], "4.1.2": [0, 3], "4.2.1": [0, 3], "4.3.1": [0, 3]}},
    8: {"name": "OffsetCommit", "versions": {"3.9.1": [0, 9], "4.1.0": [2, 9], "4.1.2": [2, 9], "4.2.1": [2, 9], "4.3.1": [2, 9]}},
    9: {"name": "OffsetFetch", "versions": {"3.9.1": [0, 9], "4.1.0": [1, 9], "4.1.2": [1, 9], "4.2.1": [1, 9], "4.3.1": [1, 9]}},
    10: {"name": "FindCoordinator", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    11: {"name": "JoinGroup", "versions": {"3.9.1": [0, 9], "4.1.0": [2, 9], "4.1.2": [2, 9], "4.2.1": [2, 9], "4.3.1": [2, 9]}},
    12: {"name": "Heartbeat", "versions": {"3.9.1": [0, 4], "4.1.0": [0, 4], "4.1.2": [0, 4], "4.2.1": [0, 4], "4.3.1": [0, 4]}},
    13: {"name": "LeaveGroup", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    14: {"name": "SyncGroup", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    15: {"name": "DescribeGroups", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 6], "4.1.2": [0, 6], "4.2.1": [0, 6], "4.3.1": [0, 6]}},
    16: {"name": "ListGroups", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    17: {"name": "SaslHandshake", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    18: {"name": "ApiVersions", "versions": {"3.9.1": [0, 3], "4.1.0": [0, 4], "4.1.2": [0, 4], "4.2.1": [0, 4], "4.3.1": [0, 4]}},
    19: {"name": "CreateTopics", "versions": {"3.9.1": [0, 7], "4.1.0": [2, 7], "4.1.2": [2, 7], "4.2.1": [2, 7], "4.3.1": [2, 7]}},
    20: {"name": "DeleteTopics", "versions": {"3.9.1": [0, 6], "4.1.0": [1, 6], "4.1.2": [1, 6], "4.2.1": [1, 6], "4.3.1": [1, 6]}},
    21: {"name": "DeleteRecords", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    22: {"name": "InitProducerId", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    23: {"name": "OffsetForLeaderEpoch", "versions": {"3.9.1": [0, 4], "4.1.0": [2, 4], "4.1.2": [2, 4], "4.2.1": [2, 4], "4.3.1": [2, 4]}},
    24: {"name": "AddPartitionsToTxn", "versions": {"3.9.1": [0, 5], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    25: {"name": "AddOffsetsToTxn", "versions": {"3.9.1": [0, 4], "4.1.0": [0, 4], "4.1.2": [0, 4], "4.2.1": [0, 4], "4.3.1": [0, 4]}},
    26: {"name": "EndTxn", "versions": {"3.9.1": [0, 4], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    27: {"name": "WriteTxnMarkers", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    28: {"name": "TxnOffsetCommit", "versions": {"3.9.1": [0, 4], "4.1.0": [0, 5], "4.1.2": [0, 5], "4.2.1": [0, 5], "4.3.1": [0, 5]}},
    29: {"name": "DescribeAcls", "versions": {"3.9.1": [0, 3], "4.1.0": [1, 3], "4.1.2": [1, 3], "4.2.1": [1, 3], "4.3.1": [1, 3]}},
    30: {"name": "CreateAcls", "versions": {"3.9.1": [0, 3], "4.1.0": [1, 3], "4.1.2": [1, 3], "4.2.1": [1, 3], "4.3.1": [1, 3]}},
    31: {"name": "DeleteAcls", "versions": {"3.9.1": [0, 3], "4.1.0": [1, 3], "4.1.2": [1, 3], "4.2.1": [1, 3], "4.3.1": [1, 3]}},
    32: {"name": "DescribeConfigs", "versions": {"3.9.1": [0, 4], "4.1.0": [1, 4], "4.1.2": [1, 4], "4.2.1": [1, 4], "4.3.1": [1, 4]}},
    33: {"name": "AlterConfigs", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    34: {"name": "AlterReplicaLogDirs", "versions": {"3.9.1": [0, 2], "4.1.0": [1, 2], "4.1.2": [1, 2], "4.2.1": [1, 2], "4.3.1": [1, 2]}},
    35: {"name": "DescribeLogDirs", "versions": {"3.9.1": [0, 4], "4.1.0": [1, 4], "4.1.2": [1, 4], "4.2.1": [1, 4], "4.3.1": [1, 5]}},
    36: {"name": "SaslAuthenticate", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    37: {"name": "CreatePartitions", "versions": {"3.9.1": [0, 3], "4.1.0": [0, 3], "4.1.2": [0, 3], "4.2.1": [0, 3], "4.3.1": [0, 3]}},
    38: {"name": "CreateDelegationToken", "versions": {"3.9.1": [0, 3], "4.1.0": [1, 3], "4.1.2": [1, 3], "4.2.1": [1, 3], "4.3.1": [1, 3]}},
    39: {"name": "RenewDelegationToken", "versions": {"3.9.1": [0, 2], "4.1.0": [1, 2], "4.1.2": [1, 2], "4.2.1": [1, 2], "4.3.1": [1, 2]}},
    40: {"name": "ExpireDelegationToken", "versions": {"3.9.1": [0, 2], "4.1.0": [1, 2], "4.1.2": [1, 2], "4.2.1": [1, 2], "4.3.1": [1, 2]}},
    41: {"name": "DescribeDelegationToken", "versions": {"3.9.1": [0, 3], "4.1.0": [1, 3], "4.1.2": [1, 3], "4.2.1": [1, 3], "4.3.1": [1, 3]}},
    42: {"name": "DeleteGroups", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    43: {"name": "ElectLeaders", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    44: {"name": "IncrementalAlterConfigs", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    45: {"name": "AlterPartitionReassignments", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    46: {"name": "ListPartitionReassignments", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    47: {"name": "OffsetDelete", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    48: {"name": "DescribeClientQuotas", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    49: {"name": "AlterClientQuotas", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    50: {"name": "DescribeUserScramCredentials", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    51: {"name": "AlterUserScramCredentials", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    52: {"name": "Vote", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    53: {"name": "BeginQuorumEpoch", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    54: {"name": "EndQuorumEpoch", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    55: {"name": "DescribeQuorum", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    56: {"name": "AlterPartition", "versions": {"3.9.1": [0, 3], "4.1.0": [0, 3], "4.1.2": [0, 3], "4.2.1": [0, 3], "4.3.1": [0, 3]}},
    57: {"name": "UpdateFeatures", "versions": {"3.9.1": [0, 2], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    58: {"name": "Envelope", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    59: {"name": "FetchSnapshot", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    60: {"name": "DescribeCluster", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 2], "4.1.2": [0, 2], "4.2.1": [0, 2], "4.3.1": [0, 2]}},
    61: {"name": "DescribeProducers", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    62: {"name": "BrokerRegistration", "versions": {"3.9.1": [0, 3], "4.1.0": [0, 3], "4.1.2": [0, 3], "4.2.1": [0, 3], "4.3.1": [0, 3]}},
    63: {"name": "BrokerHeartbeat", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    64: {"name": "UnregisterBroker", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    65: {"name": "DescribeTransactions", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    66: {"name": "ListTransactions", "versions": {"3.9.1": [0, 1], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    67: {"name": "AllocateProducerIds", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    68: {"name": "ConsumerGroupHeartbeat", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    69: {"name": "ConsumerGroupDescribe", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    70: {"name": "ControllerRegistration", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    71: {"name": "GetTelemetrySubscriptions", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    72: {"name": "PushTelemetry", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    73: {"name": "AssignReplicasToDirs", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    74: {"name": "ListClientMetricsResources", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    75: {"name": "DescribeTopicPartitions", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    76: {"name": "ShareGroupHeartbeat", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    77: {"name": "ShareGroupDescribe", "versions": {"3.9.1": [0, 0], "4.1.0": [0, 1], "4.1.2": [0, 1], "4.2.1": [0, 1], "4.3.1": [0, 1]}},
    78: {"name": "ShareFetch", "versions": {"3.9.1": [0, 0], "4.1.0": [1, 1], "4.1.2": [1, 1], "4.2.1": [1, 2], "4.3.1": [1, 2]}},
    79: {"name": "ShareAcknowledge", "versions": {"3.9.1": [0, 0], "4.1.0": [1, 1], "4.1.2": [1, 1], "4.2.1": [1, 2], "4.3.1": [1, 2]}},
    80: {"name": "AddRaftVoter", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    81: {"name": "RemoveRaftVoter", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    82: {"name": "UpdateRaftVoter", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    83: {"name": "InitializeShareGroupState", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    84: {"name": "ReadShareGroupState", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    85: {"name": "WriteShareGroupState", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    86: {"name": "DeleteShareGroupState", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
    87: {"name": "ReadShareGroupStateSummary", "versions": {"4.1.0": [0, 0], "4.1.2": [0, 0], "4.2.1": [0, 0], "4.3.1": [0, 0]}},
}

# Client spoken version ranges in partitionline
CLIENT_SPOKEN_VERSIONS: Dict[int, List[int]] = {
    0: list(range(3, 14)),   # Produce: 3-13
    1: list(range(4, 19)),   # Fetch: 4-18
    2: list(range(1, 12)),   # ListOffsets: 1-11
    3: list(range(1, 14)),   # Metadata: 1-13
    8: list(range(2, 10)),   # OffsetCommit: 2-9
    9: list(range(1, 10)),   # OffsetFetch: 1-9
    10: list(range(1, 7)),   # FindCoordinator: 1-6
    11: list(range(2, 10)),  # JoinGroup: 2-9
    12: list(range(0, 5)),   # Heartbeat: 0-4
    13: list(range(0, 6)),   # LeaveGroup: 0-5
    14: list(range(0, 6)),   # SyncGroup: 0-5
    15: list(range(0, 7)),   # DescribeGroups: 0-6
    16: list(range(0, 6)),   # ListGroups: 0-5
    17: list(range(0, 2)),   # SaslHandshake: 0-1
    18: list(range(0, 5)),   # ApiVersions: 0-4
    19: list(range(0, 8)),   # CreateTopics: 0-7
    20: list(range(0, 7)),   # DeleteTopics: 0-6
    21: list(range(0, 3)),   # DeleteRecords: 0-2
    22: list(range(0, 6)),   # InitProducerId: 0-5
    23: list(range(0, 5)),   # OffsetForLeaderEpoch: 0-4
    24: list(range(0, 4)),   # AddPartitionsToTxn: 0-3
    25: list(range(0, 5)),   # AddOffsetsToTxn: 0-4
    26: list(range(0, 6)),   # EndTxn: 0-5
    27: list(range(0, 3)),   # WriteTxnMarkers: 0-2; public Admin force-abort
    28: list(range(0, 6)),   # TxnOffsetCommit: 0-5
    29: list(range(0, 4)),   # DescribeAcls: 0-3
    30: list(range(0, 4)),   # CreateAcls: 0-3
    31: list(range(0, 4)),   # DeleteAcls: 0-3
    32: list(range(0, 5)),   # DescribeConfigs: 0-4
    33: list(range(0, 3)),   # AlterConfigs: 0-2
    34: list(range(1, 3)),   # AlterReplicaLogDirs: 1-2
    35: list(range(1, 6)),   # DescribeLogDirs: 1-5
    36: list(range(0, 3)),   # SaslAuthenticate: 0-2
    37: list(range(0, 4)),   # CreatePartitions: 0-3
    38: list(range(1, 4)),   # CreateDelegationToken: 1-3
    39: list(range(1, 3)),   # RenewDelegationToken: 1-2
    40: list(range(1, 3)),   # ExpireDelegationToken: 1-2
    41: list(range(1, 4)),   # DescribeDelegationToken: 1-3
    42: list(range(0, 3)),   # DeleteGroups: 0-2
    43: list(range(0, 3)),   # ElectLeaders: 0-2 (Admin::elect_leaders)
    44: list(range(0, 2)),   # IncrementalAlterConfigs: 0-1
    45: [0],                 # AlterPartitionReassignments: 0
    46: [0],                 # ListPartitionReassignments: 0
    47: [0],                 # OffsetDelete: 0
    48: list(range(0, 2)),   # DescribeClientQuotas: 0-1
    49: list(range(0, 2)),   # AlterClientQuotas: 0-1
    50: [0],                 # DescribeUserScramCredentials: 0
    51: [0],                 # AlterUserScramCredentials: 0
    55: list(range(0, 3)),   # DescribeQuorum: 0-2 (Admin::describe_quorum)
    57: list(range(0, 3)),   # UpdateFeatures: 0-2
    60: list(range(0, 3)),   # DescribeCluster: 0-2
    61: [0],                 # DescribeProducers: 0
    64: [0],                 # UnregisterBroker: 0
    65: [0],                 # DescribeTransactions: 0
    66: list(range(0, 2)),   # ListTransactions: 0-1
    67: [0],                 # AllocateProducerIds: 0
    68: list(range(0, 2)),   # ConsumerGroupHeartbeat: 0-1
    69: list(range(0, 2)),   # ConsumerGroupDescribe: 0-1
    71: [0],                 # GetTelemetrySubscriptions: 0
    72: [0],                 # PushTelemetry: 0
    73: [0],                 # AssignReplicasToDirs: 0
    74: list(range(0, 2)),   # ListClientMetricsResources: 0-1
    75: [0],                 # DescribeTopicPartitions: 0
    76: list(range(0, 2)),   # ShareGroupHeartbeat: 0-1
    77: list(range(0, 2)),   # ShareGroupDescribe: 0-1
    78: list(range(0, 3)),   # ShareFetch: 0-2 (KL05-15)
    79: list(range(0, 3)),   # ShareAcknowledge: 0-2 (KL05-15)
    80: [0],                 # AddRaftVoter: 0 (Admin::add_raft_voter)
    81: [0],                 # RemoveRaftVoter: 0 (Admin::remove_raft_voter)
    90: [0, 1],              # DescribeShareGroupOffsets: typed lag, legacy projection
    91: [0],                 # AlterShareGroupOffsets: 0
    92: [0],                 # DeleteShareGroupOffsets: 0
}

# Explicitly classified excluded broker-internal / clusterAction APIs.
# These are inter-broker replication, controller-internal, or Raft consensus RPCs
# that are out of scope for client libraries (recorded in api_keys.rs cluster_action
# and cases.json api-broker-internal-*).
CLASSIFIED_EXCLUDED_BROKER_INTERNAL: Dict[int, str] = {
    4: "LeaderAndIsr: inter-broker replica state propagation (clusterAction, cases.json)",
    5: "StopReplica: controller-to-broker replica deletion (clusterAction, cases.json)",
    6: "UpdateMetadata: controller-to-broker cluster state broadcast (clusterAction, cases.json)",
    7: "ControlledShutdown: broker-to-controller graceful shutdown (clusterAction, cases.json)",
    52: "Vote: KRaft consensus leader voting (clusterAction, cases.json)",
    53: "BeginQuorumEpoch: KRaft consensus leader epoch transition (clusterAction, cases.json)",
    54: "EndQuorumEpoch: KRaft consensus leader resign (clusterAction, cases.json)",
    56: "AlterPartition: broker leader ISR state update to controller (clusterAction, cases.json)",
    58: "Envelope: inter-broker request forwarding wrapper (clusterAction, cases.json)",
    59: "FetchSnapshot: KRaft controller state machine snapshot replication (features.json)",
    62: "BrokerRegistration: KRaft broker bootstrap registration (clusterAction, cases.json)",
    63: "BrokerHeartbeat: KRaft broker liveness heartbeat to controller (clusterAction, cases.json)",
    67: "AllocateProducerIds: inter-broker transactional PID block allocation (clusterAction, cases.json)",
    70: "ControllerRegistration: KRaft controller registration (features.json)",
    82: "UpdateRaftVoter: KRaft voter membership reconfig (features.json)",
    83: "InitializeShareGroupState: broker-to-persister share state storage (clusterAction, cases.json)",
    84: "ReadShareGroupState: broker-to-persister share state read (clusterAction, cases.json)",
    85: "WriteShareGroupState: broker-to-persister share state write (clusterAction, cases.json)",
    86: "DeleteShareGroupState: broker-to-persister share state deletion (clusterAction, cases.json)",
    87: "ReadShareGroupStateSummary: broker-to-persister share state summary (clusterAction, cases.json)",
}

# Non-client frameworks explicitly classified as out of scope
CLASSIFIED_OUT_OF_SCOPE_FRAMEWORKS: Dict[str, str] = {
    "streams.runtime": "Kafka Streams stream processing library out of client SDK scope (features.json)",
    "connect.framework": "Kafka Connect connector runtime framework out of client SDK scope (features.json)",
    "c_abi.librdkafka": "C rd_kafka_* ABI symbols out of pure-Rust scope (features.json)",
}

# Client APIs tracked as missing runtime wiring in features.json
# (do not count a key name as an implemented client operation).
CLASSIFIED_MISSING_RUNTIME_APIS: Dict[int, Dict[str, Any]] = {
    88: {"name": "StreamsGroupHeartbeat", "feature_id": "streams.group_heartbeat",
         "reason": "Public Streams broker protocol/client operation remains missing; Java framework exclusion does not cover API88"},
    89: {"name": "StreamsGroupDescribe", "feature_id": "streams.group_describe",
         "reason": "Public Streams group description/client operation remains missing; Java framework exclusion does not cover API89"},
}

# Classified version gaps (known differences between pinned Apache validVersions and client spoken versions).
# (api_key, version) -> reason
CLASSIFIED_VERSION_GAPS: Dict[Tuple[int, int], Dict[str, Any]] = {
    # Current official schema capabilities still absent from real client operations.
    (8, 10): {"pin": "4.1.2/4.2.1/4.3.1", "direction": "upstream_cap", "reason": "OffsetCommit v10 topic UUIDs are unsupported; runtime caps at9"},
    (9, 10): {"pin": "4.1.2/4.2.1/4.3.1", "direction": "upstream_cap", "reason": "OffsetFetch v10 topic UUIDs are unsupported; runtime caps at9"},
    (22, 6): {"pin": "4.1.2/4.2.1/4.3.1", "direction": "upstream_cap", "reason": "InitProducerId v6 two-phase transaction flags/lifecycle unsupported; runtime caps at5"},
    (45, 1): {"pin": "4.1.2/4.2.1/4.3.1", "direction": "upstream_cap", "reason": "AllowReplicationFactorChange unsupported; reassign runtime caps at0"},
    (66, 2): {"pin": "4.1.2/4.2.1/4.3.1", "direction": "upstream_cap", "reason": "TransactionalIdPattern unsupported; list-transactions runtime caps at1"},
    (80, 1): {"pin": "4.2.1/4.3.1", "direction": "upstream_cap", "reason": "AckWhenCommitted option unsupported; add-voter runtime caps at0"},

    # Produce (0)
    (0, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "Produce v0-v2 classic format removed in Kafka 4.0; client starts at v3"},
    (0, 1): {"pin": "3.9.1", "direction": "pin_only", "reason": "Produce v0-v2 classic format removed in Kafka 4.0; client starts at v3"},
    (0, 2): {"pin": "3.9.1", "direction": "pin_only", "reason": "Produce v0-v2 classic format removed in Kafka 4.0; client starts at v3"},
    (0, 12): {"pin": "3.9.1", "direction": "client_only", "reason": "Produce v12 is KIP-890 Part 2 txn V2; Kafka 3.9.1 max is v11 (Kafka 4.0+ only, cases.json)"},
    (0, 13): {"pin": "3.9.1", "direction": "client_only", "reason": "Produce v13 uses topic IDs (KIP-516); Kafka 3.9.1 max is v11. Implemented against Apache 4.1.2/4.2.1/4.3.1 fixtures with Metadata identity and request-snapshot response mapping (KL05-11)."},

    # Fetch (1)
    (1, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "Fetch v0-v3 legacy format removed in Kafka 4.0; client starts at v4"},
    (1, 1): {"pin": "3.9.1", "direction": "pin_only", "reason": "Fetch v0-v3 legacy format removed in Kafka 4.0; client starts at v4"},
    (1, 2): {"pin": "3.9.1", "direction": "pin_only", "reason": "Fetch v0-v3 legacy format removed in Kafka 4.0; client starts at v4"},
    (1, 3): {"pin": "3.9.1", "direction": "pin_only", "reason": "Fetch v0-v3 legacy format removed in Kafka 4.0; client starts at v4"},

    (1, 18): {"pin": "3.9.1", "direction": "client_only", "reason": "Fetch18 HighWatermark (KIP-1166) is implemented against Apache4.1 and current SDKs; Apache3.9.1 supports through17."},

    # ListOffsets (2)
    (2, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "ListOffsets v0 legacy format removed in Kafka 4.0; client starts at v1"},
    (2, 10): {"pin": "3.9.1", "direction": "client_only", "reason": "ListOffsets v10 adds TimeoutMs (KIP-1075); Kafka 3.9.1 max is v9 (Kafka 4.0+ only, cases.json)"},

    # Metadata (3)
    (3, 0): {"pin": "3.9.1/4.1.0+", "direction": "pin_only", "reason": "Metadata v0 legacy format; client starts at v1 (v0 rejected by builder)"},
    (3, 13): {"pin": "3.9.1", "direction": "client_only", "reason": "Metadata v13 adds top-level ErrorCode; Kafka 3.9.1 max is v12 (Kafka 4.0+ only, cases.json)"},

    # FindCoordinator (10)
    (10, 0): {"pin": "all", "direction": "pin_only", "reason": "FindCoordinator v0 is legacy GroupCoordinator without key type; client speaks 1-6"},

    # AddPartitionsToTxn (24)
    (24, 4): {"pin": "all", "direction": "upstream_cap", "reason": "AddPartitionsToTxn v4 adds batched transactions; client capped at v3 (src/protocol/header.rs)"},
    (24, 5): {"pin": "all", "direction": "upstream_cap", "reason": "AddPartitionsToTxn v5 adds batched transactions; client capped at v3 (src/protocol/header.rs)"},

    # DescribeLogDirs (35)
    (35, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "DescribeLogDirs v0 removed in Kafka 4.0; client starts at v1"},

    # Legacy versions removed in Kafka 4.0 where client still speaks classic v0/v1
    (8, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "OffsetCommit v0 removed in Kafka 4.0; client speaks 2-9"},
    (8, 1): {"pin": "3.9.1", "direction": "pin_only", "reason": "OffsetCommit v1 removed in Kafka 4.0; client speaks 2-9"},
    (9, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "OffsetFetch v0 removed in Kafka 4.0; client speaks 1-9"},
    (11, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "JoinGroup v0 removed in Kafka 4.0; client speaks 2-9"},
    (11, 1): {"pin": "3.9.1", "direction": "pin_only", "reason": "JoinGroup v1 removed in Kafka 4.0; client speaks 2-9"},
    (19, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "CreateTopics v0 removed in Kafka 4.0; client still supports legacy v0"},
    (19, 1): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "CreateTopics v1 removed in Kafka 4.0; client still supports legacy v1"},
    (20, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "DeleteTopics v0 removed in Kafka 4.0; client still supports legacy v0"},
    (23, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "OffsetForLeaderEpoch v0 removed in Kafka 4.0; client still supports legacy v0"},
    (23, 1): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "OffsetForLeaderEpoch v1 removed in Kafka 4.0; client still supports legacy v1"},
    (29, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "DescribeAcls v0 removed in Kafka 4.0; client still supports legacy v0"},
    (30, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "CreateAcls v0 removed in Kafka 4.0; client still supports legacy v0"},
    (31, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "DeleteAcls v0 removed in Kafka 4.0; client still supports legacy v0"},
    (32, 0): {"pin": "4.1.0+", "direction": "legacy_spoken", "reason": "DescribeConfigs v0 removed in Kafka 4.0; client still supports legacy v0"},
    (34, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "AlterReplicaLogDirs v0 removed in Kafka 4.0; client speaks 1-2"},
    (38, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "CreateDelegationToken v0 removed in Kafka 4.0; client speaks 1-3"},
    (39, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "RenewDelegationToken v0 removed in Kafka 4.0; client speaks 1-2"},
    (40, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "ExpireDelegationToken v0 removed in Kafka 4.0; client speaks 1-2"},
    (41, 0): {"pin": "3.9.1", "direction": "pin_only", "reason": "DescribeDelegationToken v0 removed in Kafka 4.0; client speaks 1-3"},
}


class ProtocolCoverageError(Exception):
    """Raised when unclassified schema or API drift is detected."""
    pass


def find_repo_root() -> Path:
    """Find the root directory of the partitionline repository."""
    candidates = [
        Path.cwd(),
        Path(__file__).resolve().parent.parent,
    ]
    for c in candidates:
        if (c / "Cargo.toml").is_file() and (c / "tests" / "conformance").is_dir():
            return c
    return Path.cwd()


def load_api_keys_catalog(api_keys_rs_path: Path) -> Tuple[Dict[int, str], Set[int]]:
    """Parse api_keys.rs for known API key constants, names, and clusterAction flags."""
    if not api_keys_rs_path.is_file():
        raise FileNotFoundError(f"api_keys.rs not found at {api_keys_rs_path}")

    with open(api_keys_rs_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract const NAME: i16 = VAL;
    consts: Dict[str, int] = {}
    for m in re.finditer(r"pub const ([A-Z0-9_]+):\s*i16\s*=\s*(\d+);", content):
        consts[m.group(1)] = int(m.group(2))

    # Extract name(id: i16) mapping
    name_match = re.search(
        r"pub const fn name\(id:\s*i16\)\s*->\s*Option<&'static str>\s*{(.*?)^\}",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if not name_match:
        raise ValueError("Could not parse name() function from api_keys.rs")

    names: Dict[int, str] = {}
    for line in name_match.group(1).splitlines():
        m = re.search(r"([A-Z0-9_]+|\d+)\s*=>\s*Some\(\"([A-Z0-9_]+)\"\)", line)
        if m:
            key_str, val_str = m.group(1), m.group(2)
            key_id = int(key_str) if key_str.isdigit() else consts.get(key_str)
            if key_id is not None:
                names[key_id] = val_str

    # Extract cluster_action(id: i16) mapping
    ca_match = re.search(
        r"pub const fn cluster_action\(id:\s*i16\)\s*->\s*bool\s*{(.*?)^\}",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if not ca_match:
        raise ValueError("Could not parse cluster_action() function from api_keys.rs")

    cluster_actions: Set[int] = set()
    for token in re.findall(r"([A-Z0-9_]+|\d+)", ca_match.group(1)):
        if token in ("matches", "id"):
            continue
        cid = int(token) if token.isdigit() else consts.get(token)
        if cid is not None:
            cluster_actions.add(cid)

    return names, cluster_actions


def load_features_matrix(features_path: Path) -> List[Dict[str, Any]]:
    """Load features.json matrix."""
    if not features_path.is_file():
        raise FileNotFoundError(f"features.json not found at {features_path}")
    with open(features_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("features.json must be a JSON array")
    return data


def load_cases_registry(cases_path: Path) -> Dict[str, Any]:
    """Load cases.json registry."""
    if not cases_path.is_file():
        raise FileNotFoundError(f"cases.json not found at {cases_path}")
    with open(cases_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("cases.json must be a JSON object")
    return data


# Explicit audited callable-operation bindings; a key constant/codec alone is insufficient.
CLIENT_RUNTIME_FEATURES = {0: 'producer.send',
 1: 'manual_consumer.fetch',
 2: 'manual_consumer.list_offsets_v11',
 3: 'common_admin.describe_topics',
 8: 'group.commit_sync',
 9: 'common_admin.list_consumer_group_offsets',
 10: 'group.classic_join_sync',
 11: 'group.classic_join_sync',
 12: 'group.classic_join_sync',
 13: 'group.leave_with_reason',
 14: 'group.classic_join_sync',
 15: 'common_admin.describe_groups',
 16: 'common_admin.list_groups',
 17: 'auth.sasl_scram_256',
 18: 'producer.send',
 19: 'common_admin.create_topics',
 20: 'common_admin.delete_topics',
 21: 'common_admin.delete_records',
 22: 'producer.idempotence',
 23: 'manual_consumer.offset_for_leader_epoch',
 24: 'transactions.begin',
 25: 'transactions.send_offsets',
 26: 'transactions.commit',
 27: 'full_admin.abort_transaction',
 28: 'transactions.send_offsets',
 29: 'common_admin.describe_acls',
 30: 'common_admin.create_acls',
 31: 'common_admin.delete_acls',
 32: 'common_admin.describe_configs',
 33: 'common_admin.alter_configs',
 34: 'full_admin.alter_replica_log_dirs',
 35: 'full_admin.describe_log_dirs',
 36: 'auth.sasl_scram_256',
 37: 'common_admin.create_partitions',
 38: 'full_admin.create_delegation_token',
 39: 'full_admin.renew_delegation_token',
 40: 'full_admin.expire_delegation_token',
 41: 'full_admin.describe_delegation_token',
 42: 'common_admin.delete_groups',
 43: 'full_admin.elect_leaders',
 44: 'common_admin.incremental_alter_configs',
 45: 'full_admin.alter_partition_reassignments',
 46: 'full_admin.list_partition_reassignments',
 47: 'common_admin.delete_consumer_group_offsets',
 48: 'quotas.admin_describe',
 49: 'quotas.admin_alter',
 50: 'full_admin.describe_user_scram_credentials',
 51: 'full_admin.alter_user_scram_credentials',
 55: 'full_admin.describe_quorum',
 57: 'full_admin.update_features',
 60: 'common_admin.describe_cluster',
 61: 'full_admin.describe_producers',
 64: 'full_admin.unregister_broker',
 65: 'full_admin.describe_transactions',
 66: 'full_admin.list_transactions',
 68: 'group.kip848_join',
 69: 'common_admin.describe_consumer_groups',
 71: 'full_admin.get_telemetry_subscriptions',
 72: 'full_admin.push_telemetry',
 73: 'full_admin.assign_replicas_to_dirs',
 74: 'full_admin.list_config_resources',
 75: 'common_admin.describe_topics_by_id',
 76: 'share.join',
 77: 'full_admin.describe_share_groups',
 78: 'share.poll',
 79: 'share.acknowledge',
 80: 'full_admin.add_raft_voter',
 81: 'full_admin.remove_raft_voter',
 90: 'full_admin.describe_share_group_offsets',
 91: 'full_admin.alter_share_group_offsets',
 92: 'full_admin.delete_share_group_offsets'}
PUBLIC_CLUSTER_ACTION_APIS = {27, 55, 57}
CURRENT_PINS = ("4.1.2", "4.2.1", "4.3.1")
REQUIRED_CURRENT_KEYS = set(range(93))


def load_current_inventory(matrix_path: Path) -> Dict[int, Dict[str, Any]]:
    """Reuse the existing independently anchored, bounded offline source verifier."""
    helper_path = find_repo_root() / "scripts" / "check-broker-api-matrix.py"
    namespace = {"__file__": str(helper_path), "__name__": "protocol_inventory_source"}
    exec(compile(helper_path.read_text(encoding="utf-8"), str(helper_path), "exec"), namespace)
    matrix = namespace["load_json"](matrix_path)
    releases = matrix.get("releases", [])
    if [r.get("version") for r in releases] != list(CURRENT_PINS):
        raise ProtocolCoverageError("Missing/extra/reordered current Apache release pins")
    result = copy.deepcopy(HISTORICAL_APACHE_APIS)
    for info in result.values():
        for pin in CURRENT_PINS:
            info["versions"].pop(pin, None)
    for release in releases:
        pin = release["version"]
        commit, archive_hash = namespace["TARGETS"][pin]
        if release.get("commit") != commit or release.get("source_archive_sha256") != archive_hash:
            raise ProtocolCoverageError(f"{pin}: current source pin mismatch")
        filename = f"apache-kafka-{pin}-protocol.tar.gz"
        retained_hash = namespace["RETAINED_SHA256"][pin]
        if release.get("retained_archive") != filename or release.get("retained_archive_sha256") != retained_hash:
            raise ProtocolCoverageError(f"{pin}: retained source pin mismatch")
        sources = namespace["read_archive"](matrix_path.parent / "upstream" / filename, retained_hash)
        if release.get("files_sha256") != {p: namespace["digest"](b) for p, b in sorted(sources.items())}:
            raise ProtocolCoverageError(f"{pin}: per-file source checksums mismatch")
        rows, rules = namespace["derive_inventory"](sources)
        if release.get("inventory") != rows or release.get("header_rules") != rules:
            raise ProtocolCoverageError(f"{pin}: matrix disagrees with pinned Apache source")
        if {row["api_key"] for row in rows} != REQUIRED_CURRENT_KEYS:
            raise ProtocolCoverageError(f"{pin}: incomplete current93-key inventory")
        for row in rows:
            key = row["api_key"]
            info = result.setdefault(key, {"name": row["message_name"], "versions": {}})
            raw = row["request"]["valid_versions"]
            parts = raw.split("-")
            info["versions"][pin] = None if raw == "none" else [int(parts[0]), int(parts[-1])]
            info.setdefault("current_contracts", {})[pin] = row
    return result


def check_broker_advertisements(path: Path, inventory: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Current broker profiles may advertise only active schema versions/implemented keys."""
    features = json.loads(path.read_text(encoding="utf-8"))
    states = {row["api_key"]: row["implementation"] for row in features.get("features", []) if "api_key" in row}
    drift = []
    for profile, entries in features.items():
        if not profile.endswith("api_versions") or not isinstance(entries, list):
            continue
        for entry in entries:
            key, low, high = (entry.get(k) for k in ("api_key", "min_version", "max_version"))
            valid = all(type(v) is int for v in (key, low, high)) and 0 <= low <= high
            valid = valid and key in inventory and states.get(key) in ("implemented", "partial")
            contracts = inventory.get(key, {}).get("current_contracts", {})
            if valid:
                valid = bool(contracts) and all(
                    row["disposition"] == "active" and
                    all(v in range(int(row["request"]["valid_versions"].split("-")[0]),
                                   int(row["request"]["valid_versions"].split("-")[-1]) + 1)
                        for v in range(low, high + 1)) for row in contracts.values())
            if not valid:
                drift.append({"type": "invalid_broker_advertisement", "api_key": key,
                              "profile": profile, "description": f"Invalid/removed/unimplemented API{key} advertisement in {profile}"})
    return drift


PINNED_APACHE_APIS = load_current_inventory(find_repo_root() / "tests/conformance/broker/api-matrix.json")


CONFORMANCE_MODES = ('classification', 'backlog', 'core', 'full')
CASE_PROFILES = {'core', 'transactional', 'group', 'share', 'secure', 'admin', 'ecosystem'}
CASE_DISPOSITIONS = {'independent_pass', 'local_consistency', 'failed', 'not_run',
                     'unsupported', 'blocked', 'not_applicable'}
FEATURE_CASE_PROFILES = {
    'producer': 'core', 'manual consumer': 'core', 'codecs': 'core', 'quotas': 'core',
    'transactions': 'transactional', 'group': 'group', 'share': 'share', 'auth': 'secure',
    'common admin': 'admin', 'full admin': 'admin', 'schema ecosystem': 'ecosystem',
    'streams': 'ecosystem', 'connect': 'ecosystem', 'c_abi': 'ecosystem',
    'broker-internal replication': 'admin'}
EXCLUSION_SCOPES = {'removed_current_role', 'historical_internal_role',
                    'current_broker_internal_role', 'framework_runtime', 'c_symbol_abi'}
CORE_CASE_PROFILES = {'core', 'transactional', 'group', 'admin'}


def evaluate_conformance_backlog(cases_data, features, tasks_data, repo_root):
    """Audit registered claims and open work; never execute or certify an oracle.

    The conformance booleans concern the selected registered rows only. Complete
    upstream applicability and the verified API inventory are separate gates.
    """
    issues = []
    def problem(kind, owner, description):
        issues.append({'type': kind, 'owner': owner, 'description': description})
    tasks = {}
    raw_tasks = tasks_data.get('tasks') if isinstance(tasks_data, dict) else None
    if not isinstance(raw_tasks, list):
        problem('invalid_taskbook', 'tasks', 'Expected an actual taskbook tasks array')
        raw_tasks = []
    for task in raw_tasks:
        if not isinstance(task, dict) or not isinstance(task.get('id'), str) or not task['id']:
            problem('invalid_task_id', 'tasks', 'Task ID must be a nonempty string')
            continue
        key = task['id']
        if key in tasks:
            problem('duplicate_task_id', key, 'Task ID repeated')
        if task.get('status') not in ('pending', 'in_progress', 'done'):
            problem('invalid_task_status', key, 'Unknown task status')
        tasks[key] = task.get('status')

    def has_open(owner, refs):
        if not isinstance(refs, list) or any(not isinstance(r, str) or not r for r in refs) or len(refs) != len(set(refs)):
            problem('invalid_task_bindings', owner, 'Task references must be unique nonempty strings')
            return False
        for ref in refs:
            if ref not in tasks:
                problem('unknown_task', owner, f'No actual taskbook entry for {ref}')
        return any(tasks.get(ref) in ('pending', 'in_progress') for ref in refs)

    def artifacts_valid(refs):
        if not isinstance(refs, list) or not refs or any(not isinstance(p, str) or not p for p in refs):
            return False
        root = Path(repo_root).resolve()
        for ref in refs:
            path = Path(ref)
            if path.is_absolute() or '..' in path.parts:
                return False
            target = root / path
            try:
                target.resolve(strict=True).relative_to(root)
            except (OSError, ValueError):
                return False
            if target.is_symlink() or not target.is_file():
                return False
        return True

    cases = cases_data.get('cases', [])
    if not isinstance(cases, list):
        problem('invalid_case_registry', 'cases', 'Cases must be an array')
        cases = []
    if not cases:
        problem('empty_case_registry', 'cases', 'An empty registry cannot establish coverage')
    case_by_id = {}
    qualified = set()
    required = []
    excluded = []
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get('id'), str) or not case['id']:
            problem('invalid_case_id', 'cases', 'Case ID must be a nonempty string')
            continue
        key = case['id']
        if key in case_by_id:
            problem('duplicate_case_id', key, 'Case ID repeated')
        case_by_id[key] = case
        disposition = case.get('disposition')
        profile = case.get('profile')
        if disposition not in CASE_DISPOSITIONS:
            problem('invalid_case_disposition', key, 'Unknown case disposition')
        if profile not in CASE_PROFILES:
            problem('invalid_case_profile', key, 'Unknown case profile')
        if type(case.get('denominator')) is not bool:
            problem('invalid_denominator', key, 'Denominator must be a boolean')
            continue
        api_key = case.get('api_key')
        family = case.get('api_family')
        if family is not None:
            family_key = next((key for key, info in PINNED_APACHE_APIS.items() if info['name'] == family), None)
            if family_key is None:
                problem('unknown_case_api', key, 'Unknown API family in a registered case')
            elif api_key is not None and api_key != family_key:
                problem('case_api_mismatch', key, 'Case API key disagrees with its authoritative family')
            elif api_key is None:
                api_key = family_key
        version = case.get('api_version')
        if version is not None and (type(version) is not int or version < 0):
            problem('invalid_case_version', key, 'Case version must be null or a nonnegative integer')
        if api_key is None:
            match = re.match(r'(?:api-broker-internal|current-public-api|current-claimed-api)-(\d+)-', key)
            api_key = int(match.group(1)) if match else None
        if api_key is not None and (type(api_key) is not int or api_key not in REQUIRED_CURRENT_KEYS):
            problem('unknown_case_api', key, 'Case API must belong to the authoritative current inventory')
        if not case['denominator']:
            excluded.append(key)
            scope = case.get('applicability_scope')
            role_is_valid = (
                (scope == 'removed_current_role' and api_key in (4, 5, 6, 7)) or
                (scope == 'historical_internal_role' and api_key in (27, 55, 57, 67)) or
                (scope == 'current_broker_internal_role' and api_key in CLASSIFIED_EXCLUDED_BROKER_INTERNAL and
                 api_key not in (4, 5, 6, 7, 67)) or
                (scope in ('framework_runtime', 'c_symbol_abi') and profile == 'ecosystem' and
                 case.get('applicability_role') in ('streams_framework_runtime', 'connect_framework_runtime', 'c_symbol_abi')))
            if (disposition != 'not_applicable' or scope not in EXCLUSION_SCOPES or
                    not isinstance(case.get('applicability_reason'), str) or not case['applicability_reason'].strip() or
                    not role_is_valid or
                    (api_key in (27, 55, 57, 67) and scope != 'historical_internal_role')):
                problem('invalid_case_exclusion', key, 'Only a specifically inapplicable source role may leave the denominator')
        else:
            required.append(key)
            if disposition == 'not_applicable':
                problem('invalid_case_exclusion', key, 'An inapplicable row must declare its excluded source role')
            if api_key in (67, 73) and case.get('applicability_role') != 'Rust_raw_extension_no_Java_Admin_equivalent':
                problem('invalid_raw_extension_role', key, 'Raw extensions have no standard public Java Admin counterpart')
            if disposition == 'independent_pass':
                if artifacts_valid(case.get('artifacts')):
                    qualified.add(key)
                else:
                    problem('invalid_independent_artifacts', key, 'Independent attribution requires local regular artifact files')
            elif not has_open(key, case.get('backlog_tasks', [])):
                problem('untracked_case', key, 'Remaining case needs an actual pending or in-progress task')
        if 'backlog_tasks' in case and disposition == 'independent_pass':
            has_open(key, case['backlog_tasks'])  # Validate optional references; done provenance is allowed.
    for key in excluded:
        case = case_by_id[key]
        match = re.match(r'api-broker-internal-(\d+)-', key)
        api_key = case.get('api_key', int(match.group(1)) if match else None)
        if api_key in (27, 55, 57, 67):
            if not any(case_by_id[c].get('api_key') == api_key or
                       re.match(rf'current-(?:public|claimed)-api-{api_key:03d}-', c)
                       for c in required):
                problem('missing_current_claimed_cases', key, 'Historical internal exclusion needs separately required current operation cells')

    feature_ids = set()
    unqualified_features = []
    feature_profiles = {}
    for feature in features:
        if not isinstance(feature, dict) or not isinstance(feature.get('id'), str) or not feature['id']:
            problem('invalid_feature_id', 'features', 'Feature ID must be a nonempty string')
            continue
        key = feature['id']
        if key in feature_ids:
            problem('duplicate_feature_id', key, 'Feature ID repeated')
        feature_ids.add(key)
        profile = feature.get('profile')
        feature_profiles[key] = FEATURE_CASE_PROFILES.get(profile)
        if profile not in FEATURE_CASE_PROFILES:
            problem('invalid_feature_profile', key, 'Unknown feature profile')
        disposition = feature.get('disposition')
        if disposition == 'out_of_scope':
            if (feature.get('applicability_scope') not in EXCLUSION_SCOPES or
                    not isinstance(feature.get('applicability_reason'), str) or not feature['applicability_reason'].strip() or
                    not (key in CLASSIFIED_OUT_OF_SCOPE_FRAMEWORKS or key.startswith('broker_internal.'))):
                problem('invalid_feature_exclusion', key, 'Feature exclusion must name a limited source role')
            continue
        if disposition not in ('present', 'missing', 'partial'):
            problem('invalid_feature_disposition', key, 'Unknown feature disposition')
        if disposition != 'present':
            unqualified_features.append(key)
            if not has_open(key, feature.get('backlog_tasks', [])):
                problem('untracked_feature', key, 'Missing/partial feature needs an actual open task')
            continue
        refs = feature.get('conformance_case_ids', [])
        valid_refs = isinstance(refs, list) and all(isinstance(r, str) and r for r in refs) and len(refs)==len(set(refs))
        if not valid_refs or any(r not in case_by_id for r in refs):
            problem('invalid_feature_case_binding', key, 'Feature cases must refer to unique registered case IDs')
            refs = []
        if not refs or any(r not in qualified or case_by_id[r].get('profile') != feature_profiles[key] for r in refs):
            unqualified_features.append(key)
            if not has_open(key, feature.get('qualification_backlog_tasks', [])):
                problem('untracked_feature_qualification', key, 'Implementation evidence alone does not qualify independent behavior')

    contract = cases_data.get('coverage_contract', {})
    if not isinstance(contract, dict):
        problem('invalid_coverage_contract', 'registry', 'Coverage contract must be an object')
        contract = {}
    for field, actual, kind in (('required_case_ids', set(case_by_id), 'case_registry_mismatch'),
                                ('denominator_case_ids', set(required), 'denominator_registry_mismatch'),
                                ('required_feature_ids', feature_ids, 'feature_registry_mismatch')):
        if field in contract:
            refs = contract[field]
            if not isinstance(refs, list) or any(not isinstance(r, str) for r in refs) or len(refs)!=len(set(refs)) or set(refs)!=actual:
                problem(kind, 'registry', 'Registered IDs disagree with the declared audited set')
    exhaustive = (contract.get('upstream_applicability_complete') is True and
                  artifacts_valid(contract.get('upstream_applicability_artifacts')))
    if contract and not exhaustive and not has_open('upstream_applicability', contract.get('applicability_backlog_tasks', [])):
        problem('untracked_upstream_applicability', 'registry', 'Incomplete source/parameter applicability needs an open task')
    unresolved = sorted(set(required)-qualified)
    core_unresolved = [key for key in unresolved if case_by_id[key].get('profile') in CORE_CASE_PROFILES]
    core_features = [key for key in unqualified_features if feature_profiles.get(key) in CORE_CASE_PROFILES]
    return {'issues': issues, 'total_cases': len(cases), 'required_cases': len(required),
            'excluded_cases': len(excluded), 'independent_cases': len(qualified),
            'unqualified_cases': len(unresolved), 'unqualified_case_ids': unresolved,
            'unqualified_feature_ids': sorted(unqualified_features),
            'backlog_complete': not issues,
            'core_conformance_complete': bool(required) and not issues and not core_unresolved and not core_features,
            'full_conformance_complete': bool(required) and not issues and not unresolved and not unqualified_features,
            'exhaustive_upstream_applicability_complete': exhaustive,
            'qualification_limit': 'Structural registry/task attribution only; original independent oracle validation remains required.'}


def evaluate_protocol_coverage(
    inventory: Optional[Dict[int, Dict[str, Any]]] = None,
    api_keys_path: Optional[Path] = None,
    features_path: Optional[Path] = None,
    cases_path: Optional[Path] = None,
    extra_classified_apis: Optional[Dict[int, str]] = None,
    extra_classified_versions: Optional[Dict[Tuple[int, int], str]] = None,
    broker_features_path: Optional[Path] = None,
    tasks_path: Optional[Path] = None,
    mode: str = "classification",
) -> Dict[str, Any]:
    """
    Deterministically compare Apache schema/API inventory with codec, runtime,
    and test coverage.

    Returns structured results with gap counts and any unclassified drift.
    """
    if mode not in CONFORMANCE_MODES:
        raise ProtocolCoverageError(f"Unknown coverage mode: {mode}")
    repo_root = find_repo_root()
    if api_keys_path is None:
        api_keys_path = repo_root / "src" / "protocol" / "api_keys.rs"
    if features_path is None:
        features_path = repo_root / "tests" / "conformance" / "features.json"
    if cases_path is None:
        cases_path = repo_root / "tests" / "conformance" / "cases.json"

    # Load repo assets
    catalog_names, cluster_actions = load_api_keys_catalog(api_keys_path)
    features_list = load_features_matrix(features_path)
    cases_data = load_cases_registry(cases_path)

    # Active pinned inventory to verify (default to PINNED_APACHE_APIS)
    target_inventory = copy.deepcopy(PINNED_APACHE_APIS if inventory is None else inventory)

    # Merge classifications with any user-supplied extras
    excluded_broker_internal = dict(CLASSIFIED_EXCLUDED_BROKER_INTERNAL)
    missing_runtime_apis = dict(CLASSIFIED_MISSING_RUNTIME_APIS)
    version_gaps_map = dict(CLASSIFIED_VERSION_GAPS)

    classification_errors = []
    if extra_classified_apis:
        for k, classification_type in extra_classified_apis.items():
            if type(k) is not int or classification_type not in ("excluded_broker_internal", "missing_runtime"):
                classification_errors.append({"type": "unknown_classification", "api_key": k, "description": "Unknown API classification intent"})
                continue
            if classification_type == "excluded_broker_internal":
                excluded_broker_internal[k] = "User-classified broker-internal API"
            elif classification_type == "missing_runtime":
                missing_runtime_apis[k] = {
                    "name": target_inventory.get(k, {}).get("name", f"API-{k}"),
                    "feature_id": f"custom.missing_api_{k}",
                    "reason": "User-classified missing runtime wiring",
                }

    if extra_classified_versions:
        for (k, v), reason in extra_classified_versions.items():
            version_gaps_map[(k, v)] = {
                "pin": "user",
                "direction": "user_gap",
                "reason": reason,
            }

    # Tracking lists
    implemented_client_apis: List[Dict[str, Any]] = []
    missing_runtime_wiring: List[Dict[str, Any]] = []
    excluded_broker_internal_list: List[Dict[str, Any]] = []
    version_gaps_list: List[Dict[str, Any]] = []
    unclassified_drift: List[Dict[str, Any]] = list(classification_errors)

    for key, expected in PINNED_APACHE_APIS.items():
        actual = target_inventory.get(key)
        if actual is None:
            continue
        for pin in CURRENT_PINS:
            if (actual.get("versions", {}).get(pin) != expected.get("versions", {}).get(pin) or
                    actual.get("current_contracts", {}).get(pin) != expected.get("current_contracts", {}).get(pin)):
                unclassified_drift.append({"type": "pinned_inventory_mismatch", "api_key": key, "pin": pin,
                                          "description": f"API{key} {pin} disagrees with verified upstream contract"})
    missing_keys = REQUIRED_CURRENT_KEYS - target_inventory.keys()
    for key in sorted(missing_keys):
        unclassified_drift.append({"type": "missing_inventory_key", "api_key": key,
                                  "description": f"Required current Apache API{key} omitted from inventory"})
    for key in sorted(PUBLIC_CLUSTER_ACTION_APIS & excluded_broker_internal.keys()):
        unclassified_drift.append({"type": "public_api_excluded", "api_key": key,
                                  "description": f"Public Admin API{key} incorrectly excluded solely by clusterAction"})
    unclassified_drift.extend(check_broker_advertisements(
        broker_features_path or repo_root / "tests/conformance/broker/features.json", target_inventory))

    # Map features for runtime wiring checks
    features_by_id = {f["id"]: f for f in features_list}
    missing_features_runtime = [
        f for f in features_list
        if f.get("disposition") == "missing" and f.get("layer") == "runtime"
    ]
    partial_features_runtime = [
        f for f in features_list
        if f.get("disposition") == "partial" and f.get("layer") == "runtime"
    ]
    out_of_scope_features = [
        f for f in features_list
        if f.get("disposition") == "out_of_scope"
    ]

    # Evaluate all APIs in the inventory
    for api_key, api_info in sorted(target_inventory.items()):
        api_name = api_info.get("name") or catalog_names.get(api_key, f"API_{api_key}")
        pinned_versions = api_info.get("versions", {})

        # 1. Is it an excluded broker-internal API?
        if api_key in excluded_broker_internal:
            excluded_broker_internal_list.append({
                "api_key": api_key,
                "name": api_name,
                "cluster_action": api_key in cluster_actions,
                "reason": excluded_broker_internal[api_key],
            })
            continue

        # 2. Is it a classified missing runtime wiring API?
        # Acceptance: Do not count a key name or helper type as an implemented client operation!
        if api_key in missing_runtime_apis:
            info = missing_runtime_apis[api_key]
            if api_key in CLIENT_SPOKEN_VERSIONS:
                unclassified_drift.append({"type": "runtime_unverified", "api_key": api_key,
                                          "description": f"API{api_key} claims client support without a declared callable runtime operation"})
            missing_runtime_wiring.append({
                "api_key": api_key,
                "name": api_name,
                "feature_id": info.get("feature_id"),
                "has_key_name_in_catalog": api_key in catalog_names,
                "has_wire_helper": api_key in CLIENT_SPOKEN_VERSIONS,
                "has_runtime_operation": False,
                "reason": info.get("reason"),
            })
            continue

        # 3. Is it an implemented client operation?
        # Verify that it is NOT a key name or helper type masquerading as an operation.
        spoken_versions = CLIENT_SPOKEN_VERSIONS.get(api_key)
        if spoken_versions is not None:
            feature_id = CLIENT_RUNTIME_FEATURES.get(api_key)
            binding = features_by_id.get(feature_id, {})
            entrypoint = binding.get("entrypoint", "")
            if (binding.get("layer") != "runtime" or binding.get("disposition") != "present" or
                    not entrypoint.startswith("partitionline::") or "::protocol::" in entrypoint):
                unclassified_drift.append({"type": "runtime_unverified", "api_key": api_key,
                                          "description": f"API{api_key} lacks its declared callable runtime feature {feature_id}"})
                continue
            # Callable feature attribution is independent of key-name/codec membership.
            implemented_client_apis.append({
                "api_key": api_key,
                "name": api_name,
                "spoken_versions": spoken_versions,
                "runtime_feature": feature_id,
                "entrypoint": entrypoint,
                "internal_use": api_key in cluster_actions,
                "client_advertised_min": min(spoken_versions),
                "client_advertised_max": max(spoken_versions),
            })

            # Check versions against pins for version gaps and drift
            for pin_name, v_range in sorted(pinned_versions.items()):
                if not (isinstance(v_range, (list, tuple)) and len(v_range) == 2):
                    continue
                min_v, max_v = v_range[0], v_range[1]
                for v in range(min_v, max_v + 1):
                    if v not in spoken_versions:
                        # Version not spoken by client - check if classified version gap
                        gap_key = (api_key, v)
                        if gap_key in version_gaps_map:
                            gap_info = version_gaps_map[gap_key]
                            # Only add unique entries per (api_key, version)
                            if not any(g["api_key"] == api_key and g["version"] == v for g in version_gaps_list):
                                version_gaps_list.append({
                                    "api_key": api_key,
                                    "name": api_name,
                                    "version": v,
                                    "pin": pin_name,
                                    "direction": gap_info.get("direction", "gap"),
                                    "reason": gap_info.get("reason"),
                                })
                        else:
                            # UNCLASSIFIED VERSION DRIFT!
                            unclassified_drift.append({
                                "type": "unclassified_version",
                                "api_key": api_key,
                                "name": api_name,
                                "version": v,
                                "pin": pin_name,
                                "description": f"Unclassified version {v} for {api_name} (pin {pin_name})",
                            })

            # Also check if client speaks versions older than pin supported
            for v in spoken_versions:
                gap_key = (api_key, v)
                if gap_key in version_gaps_map:
                    gap_info = version_gaps_map[gap_key]
                    if not any(g["api_key"] == api_key and g["version"] == v for g in version_gaps_list):
                        version_gaps_list.append({
                            "api_key": api_key,
                            "name": api_name,
                            "version": v,
                            "pin": "all",
                            "direction": gap_info.get("direction", "legacy_spoken"),
                            "reason": gap_info.get("reason"),
                        })
            continue

        # 4. If we reach here, this API is UNCLASSIFIED in the inventory!
        unclassified_drift.append({
            "type": "unclassified_api",
            "api_key": api_key,
            "name": api_name,
            "description": f"Unclassified API key {api_key} ({api_name}) in pinned inventory",
        })

    # Record non-API runtime feature gaps from features.json
    feature_runtime_gaps: List[Dict[str, Any]] = []
    for f in missing_features_runtime:
        # Avoid duplicating the admin APIs already in missing_runtime_wiring
        ev = f.get("evidence", "")
        if f["id"] in {info.get("feature_id") for info in missing_runtime_apis.values()} or any(f"api_keys.rs:{k}" in ev for k in missing_runtime_apis):
            continue
        feature_runtime_gaps.append({
            "feature_id": f["id"],
            "profile": f.get("profile"),
            "disposition": "missing",
            "notes": f.get("notes"),
        })

    feature_partial_runtime: List[Dict[str, Any]] = []
    for f in partial_features_runtime:
        feature_partial_runtime.append({
            "feature_id": f["id"],
            "profile": f.get("profile"),
            "disposition": "partial",
            "notes": f.get("notes"),
        })

    # Conformance case registry cross-check
    cases_list = cases_data.get("cases", [])
    total_cases = len(cases_list)
    denominator_cases = sum(1 for c in cases_list if c.get("denominator", True))
    excluded_cases = total_cases - denominator_cases

    # Verification: Ensure no key name or helper type is reported as an implemented client operation
    for m in missing_runtime_wiring:
        if any(impl["api_key"] == m["api_key"] for impl in implemented_client_apis):
            raise AssertionError(f"Key name for API {m['api_key']} was incorrectly counted as an implemented client operation!")
    for b in excluded_broker_internal_list:
        if any(impl["api_key"] == b["api_key"] for impl in implemented_client_apis):
            raise AssertionError(f"Broker-internal API {b['api_key']} was incorrectly counted as an implemented client operation!")

    drift_detected = len(unclassified_drift) > 0
    exit_code = 1 if drift_detected else 0

    gap_counts = {
        "version_gaps": len(version_gaps_list),
        "missing_runtime_wiring_apis": len(missing_runtime_wiring),
        "missing_runtime_features": len(feature_runtime_gaps),
        "partial_runtime_features": len(feature_partial_runtime),
        "excluded_broker_internal_apis": len(excluded_broker_internal_list),
        "excluded_frameworks": len(CLASSIFIED_OUT_OF_SCOPE_FRAMEWORKS),
        "implemented_client_apis": len(implemented_client_apis),
        "unclassified_drift": len(unclassified_drift),
    }

    results = {
        "schema_version": 1,
        "audited_source": cases_data.get("audited_source", "cb7e97d3b92a8555aea34d59266a2990c206395f"),
        "frozen_pins": FROZEN_PINS,
        "current_inventory_source": "tests/conformance/broker/api-matrix.json; retained Apache source verified offline",
        "current_release_contracts": {
            str(key): info.get("current_contracts", {}) for key, info in sorted(target_inventory.items())
        },
        "full_current_protocol_complete": not missing_runtime_wiring and not any(
            gap["direction"] == "upstream_cap" for gap in version_gaps_list) and not unclassified_drift,
        "summary": {
            "total_pinned_apis": len(target_inventory),
            "total_catalog_keys": len(catalog_names),
            "implemented_client_apis_count": len(implemented_client_apis),
            "version_gaps_count": len(version_gaps_list),
            "missing_runtime_wiring_count": len(missing_runtime_wiring) + len(feature_runtime_gaps),
            "excluded_broker_internal_count": len(excluded_broker_internal_list),
            "unclassified_drift_count": len(unclassified_drift),
            "drift_detected": drift_detected,
            "status": "FAIL (unclassified drift detected)" if drift_detected else "PASS (all APIs and versions classified)",
            "exit_code": exit_code,
        },
        "gap_counts": gap_counts,
        "implemented_client_operations": implemented_client_apis,
        "version_gaps": sorted(version_gaps_list, key=lambda x: (x["api_key"], x["version"])),
        "missing_runtime_wiring": {
            "unimplemented_apis": missing_runtime_wiring,
            "missing_features": feature_runtime_gaps,
            "partial_features": feature_partial_runtime,
        },
        "excluded_broker_internal": {
            "broker_internal_apis": excluded_broker_internal_list,
            "out_of_scope_frameworks": CLASSIFIED_OUT_OF_SCOPE_FRAMEWORKS,
        },
        "unclassified_drift": unclassified_drift,
        "conformance_cases_summary": {
            "total_cases": total_cases,
            "denominator_cases": denominator_cases,
            "excluded_cases": excluded_cases,
        },
    }

    taskbook_path = tasks_path or repo_root / "docs/plan/tasks.json"
    tasks_data = json.loads(taskbook_path.read_text(encoding="utf-8"))
    conformance = evaluate_conformance_backlog(cases_data, features_list, tasks_data, repo_root)
    contract = cases_data.get('coverage_contract', {})
    if not isinstance(contract, dict) or not {
            'required_case_ids', 'denominator_case_ids', 'required_feature_ids',
            'upstream_applicability_complete'}.issubset(contract):
        conformance['issues'].append({'type': 'missing_coverage_contract', 'owner': 'registry',
                                     'description': 'Strict coverage needs an explicit audited row and denominator contract'})
        for verdict in ('backlog_complete', 'core_conformance_complete', 'full_conformance_complete'):
            conformance[verdict] = False
    api_implementation_complete = results["full_current_protocol_complete"]
    authoritative_inventory = target_inventory == PINNED_APACHE_APIS
    results["mode"] = mode
    results["authoritative_inventory"] = authoritative_inventory
    results["qualification_scopes"] = {
        'core_case_profiles': sorted(CORE_CASE_PROFILES), 'full_case_profiles': sorted(CASE_PROFILES),
        'feature_profile_mapping': FEATURE_CASE_PROFILES,
        'complete_upstream_scope': conformance['exhaustive_upstream_applicability_complete']}
    results["current_api_implementation_complete"] = api_implementation_complete
    results["conformance_backlog"] = conformance
    core_runtime_complete = (not any(
        FEATURE_CASE_PROFILES.get(features_by_id.get(row.get('feature_id'), {}).get('profile')) in CORE_CASE_PROFILES
        for row in missing_runtime_wiring) and not any(
        gap['direction'] == 'upstream_cap' and
        FEATURE_CASE_PROFILES.get(features_by_id.get(CLIENT_RUNTIME_FEATURES.get(gap['api_key']), {}).get('profile')) in CORE_CASE_PROFILES
        for gap in version_gaps_list))
    results["core_protocol_complete"] = (conformance["core_conformance_complete"] and
        conformance["exhaustive_upstream_applicability_complete"] and core_runtime_complete and
        authoritative_inventory and not unclassified_drift)
    results["full_current_protocol_complete"] = (api_implementation_complete and authoritative_inventory and
        conformance["full_conformance_complete"] and conformance["exhaustive_upstream_applicability_complete"])
    if mode != "classification":
        succeeded = authoritative_inventory and not unclassified_drift and conformance["backlog_complete"]
        if mode == "core":
            succeeded = succeeded and results["core_protocol_complete"]
        elif mode == "full":
            succeeded = succeeded and results["full_current_protocol_complete"]
        results["summary"]["exit_code"] = 0 if succeeded else 1
        results["summary"]["status"] = ("PASS" if succeeded else "FAIL") + f" ({mode} coverage gate)"
    return results


def format_human_report(results: Dict[str, Any], diff_only: bool = False) -> str:
    """Format human-readable summary table."""
    summary = results["summary"]
    gap_counts = results["gap_counts"]
    lines = [
        "============================================================",
        "          PROTOCOL SCHEMA & COVERAGE DRIFT REPORT",
        "============================================================",
        f"Coverage Mode:              {results.get('mode', 'classification')}",
        f"Frozen Apache Pins:         {', '.join(results['frozen_pins'])}",
        f"Total Pinned Apache APIs:   {summary['total_pinned_apis']}",
        f"Total Catalog Key Names:    {summary['total_catalog_keys']}",
        f"Implemented Client APIs:    {summary['implemented_client_apis_count']}",
        f"Version Gaps:               {gap_counts['version_gaps']}",
        f"Missing Runtime Wiring APIs:{gap_counts['missing_runtime_wiring_apis']}",
        f"Missing Runtime Features:   {gap_counts['missing_runtime_features']}",
        f"Partial Runtime Features:   {gap_counts['partial_runtime_features']}",
        f"Excluded Broker-Internal:   {gap_counts['excluded_broker_internal_apis']} APIs + {gap_counts['excluded_frameworks']} frameworks",
        f"Unclassified Drift Items:   {gap_counts['unclassified_drift']}",
        "------------------------------------------------------------",
    ]

    if not diff_only:
        lines.append(f"Implemented Client APIs ({len(results['implemented_client_operations'])}):")
        for op in results["implemented_client_operations"]:
            v_str = f"v{op['client_advertised_min']}-v{op['client_advertised_max']}"
            lines.append(f"  [{op['api_key']:2d}] {op['name']:<28} ({v_str})")
        lines.append("------------------------------------------------------------")

    lines.append(f"Version Gaps ({len(results['version_gaps'])}):")
    for vg in results["version_gaps"]:
        lines.append(f"  * [{vg['api_key']:2d}] {vg['name']} v{vg['version']}: {vg['reason']}")
    lines.append("------------------------------------------------------------")

    lines.append(f"Missing Runtime Wiring APIs ({len(results['missing_runtime_wiring']['unimplemented_apis'])}):")
    for ma in results["missing_runtime_wiring"]["unimplemented_apis"]:
        lines.append(f"  * [{ma['api_key']:2d}] {ma['name']}: {ma['reason']}")
    lines.append("------------------------------------------------------------")

    lines.append(f"Excluded Broker-Internal APIs ({len(results['excluded_broker_internal']['broker_internal_apis'])}):")
    for bi in results["excluded_broker_internal"]["broker_internal_apis"]:
        lines.append(f"  * [{bi['api_key']:2d}] {bi['name']}: {bi['reason']}")
    lines.append("------------------------------------------------------------")

    if results["unclassified_drift"]:
        lines.append(f"UNCLASSIFIED DRIFT DETECTED ({len(results['unclassified_drift'])} items):")
        for drift in results["unclassified_drift"]:
            lines.append(f"  ! {drift['description']}")
        lines.append("------------------------------------------------------------")

    lines.append(f"Final Status: {summary['status']} [exit={summary['exit_code']}]")
    lines.append("============================================================")
    return "\n".join(lines)


def run_self_tests() -> int:
    """Run built-in self-tests including synthetic injection failure verification."""
    print("Running protocol coverage self-tests...")

    # 1. Test clean run on frozen pins
    res = evaluate_protocol_coverage()
    assert res["summary"]["exit_code"] == 0, "Frozen pins must exit 0"
    assert len(res["unclassified_drift"]) == 0, "Frozen pins must have 0 unclassified drift"
    assert res["gap_counts"]["missing_runtime_wiring_apis"] == 2, "Streams88/89 remain actual missing runtime APIs"
    assert res["gap_counts"]["excluded_broker_internal_apis"] == 20, "Public27/57 must not be wholly excluded"
    print("  [ok] Frozen pins evaluated cleanly (exit 0, 0 unclassified drift)")

    # 2. Test synthetic new API injection fails
    synthetic_inventory = copy.deepcopy(PINNED_APACHE_APIS)
    synthetic_inventory[99] = {"name": "SyntheticNewApi", "versions": {"4.3.1": [0, 1]}}
    res_synthetic_api = evaluate_protocol_coverage(inventory=synthetic_inventory)
    assert res_synthetic_api["summary"]["exit_code"] == 1, "Synthetic API injection must exit 1"
    assert res_synthetic_api["summary"]["drift_detected"] is True, "Drift must be detected for synthetic API"
    assert any(d["type"] == "unclassified_api" and d["api_key"] == 99 for d in res_synthetic_api["unclassified_drift"])
    print("  [ok] Synthetic unclassified API injection rejected (exit 1)")

    # 3. Test synthetic new API passes once classified
    res_classified_api = evaluate_protocol_coverage(
        inventory=synthetic_inventory,
        extra_classified_apis={99: "missing_runtime"}
    )
    assert res_classified_api["summary"]["exit_code"] == 0, "Classified synthetic API must exit 0"
    assert len(res_classified_api["unclassified_drift"]) == 0, "Classified synthetic API must have 0 drift"
    print("  [ok] Classified synthetic API accepted (exit 0)")

    # 4. Test synthetic new version injection fails
    synthetic_version_inventory = copy.deepcopy(PINNED_APACHE_APIS)
    synthetic_version_inventory[0]["versions"]["synthetic"] = [3, 14]  # Produce v14 (unclassified!)
    res_synthetic_version = evaluate_protocol_coverage(inventory=synthetic_version_inventory)
    assert res_synthetic_version["summary"]["exit_code"] == 1, "Synthetic version injection must exit 1"
    assert res_synthetic_version["summary"]["drift_detected"] is True, "Drift must be detected for synthetic version"
    assert any(d["type"] == "unclassified_version" and d["version"] == 14 for d in res_synthetic_version["unclassified_drift"])
    print("  [ok] Synthetic unclassified version injection rejected (exit 1)")

    # 5. Test synthetic new version passes once classified
    res_classified_version = evaluate_protocol_coverage(
        inventory=synthetic_version_inventory,
        extra_classified_versions={(0, 14): "Classified experimental produce v14"}
    )
    assert res_classified_version["summary"]["exit_code"] == 0, "Classified synthetic version must exit 0"
    assert len(res_classified_version["unclassified_drift"]) == 0, "Classified synthetic version must have 0 drift"
    print("  [ok] Classified synthetic version accepted (exit 0)")

    # 6. Verify key names in catalog are NOT counted as implemented operations
    for op in res["implemented_client_operations"]:
        assert op["api_key"] not in CLASSIFIED_MISSING_RUNTIME_APIS, f"Missing API {op['api_key']} counted as implemented"
        assert op["api_key"] not in CLASSIFIED_EXCLUDED_BROKER_INTERNAL, f"Internal API {op['api_key']} counted as implemented"
    print("  [ok] Key names and helper types strictly excluded from implemented client operations")

    print("Protocol coverage self-tests: ALL PASSED")
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic protocol schema and API coverage drift checker (KL01-11)"
    )
    parser.add_argument(
        "-i",
        "--inventory",
        type=Path,
        default=None,
        help="Path to custom/synthetic pinned schema inventory JSON file",
    )
    parser.add_argument(
        "-r",
        "--registry",
        type=Path,
        default=None,
        help="Path to cases.json (defaults to auto-discovery)",
    )
    parser.add_argument(
        "-f",
        "--features",
        type=Path,
        default=None,
        help="Path to features.json (defaults to auto-discovery)",
    )
    parser.add_argument(
        "-a",
        "--api-keys",
        type=Path,
        default=None,
        help="Path to src/protocol/api_keys.rs (defaults to auto-discovery)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write result JSON to specified file path",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output to stdout",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress human-readable report text",
    )
    parser.add_argument(
        "--diff-only",
        action="store_true",
        help="Print only differences and gaps",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in self-tests and exit",
    )
    parser.add_argument("--mode", choices=CONFORMANCE_MODES, default="classification",
                        help="Classification is compatible; backlog/core/full enforce remaining work and qualification")
    parser.add_argument("--tasks", type=Path, default=None, help="Actual plan taskbook for backlog bindings")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.self_test:
        return run_self_tests()

    custom_inventory = None
    if args.inventory:
        if not args.inventory.is_file():
            sys.stderr.write(f"Error: inventory file not found: {args.inventory}\n")
            return 2
        try:
            with open(args.inventory, "r", encoding="utf-8") as f:
                raw_inv = json.load(f)
            # Normalize keys to int
            custom_inventory = {int(k): v for k, v in raw_inv.items()}
        except Exception as e:
            sys.stderr.write(f"Error parsing inventory JSON: {e}\n")
            return 2

    try:
        results = evaluate_protocol_coverage(
            inventory=custom_inventory,
            api_keys_path=args.api_keys,
            features_path=args.features,
            cases_path=args.registry,
            tasks_path=args.tasks,
            mode=args.mode,
        )
    except Exception as e:
        sys.stderr.write(f"Error evaluating protocol coverage: {e}\n")
        return 2

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            sys.stderr.write(f"Error writing output to {args.output}: {e}\n")
            return 2

    if args.json:
        print(json.dumps(results, indent=2))
    elif not args.quiet:
        print(format_human_report(results, diff_only=args.diff_only))

    return results["summary"]["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
