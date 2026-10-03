#!/bin/sh
# Runtime CPU lease override; frozen oracle source arguments remain unchanged.
set -eu
if [ "$#" -lt 3 ] || [ "$1" != "-c" ] || [ "$2" != "0-2,4" ]; then
    echo 'Unexpected taskset invocation in compaction oracle wrapper' >&2
    exit 64
fi
: "${PARTITIONLINE_ORACLE_AFFINITY_LOG:?Missing affinity audit path}"
printf 'requested=0-2,4 effective=2,4 executable=%s\n' "$3" >> "$PARTITIONLINE_ORACLE_AFFINITY_LOG"
shift 2
exec /usr/bin/taskset -c 2,4 "$@"
