#!/usr/bin/env bash
# Runs every upq_bench scenario in its own process (one uvent runtime per process).
#   run_bench.sh <upq_bench binary> <host> <port> <user> <password> <db> [seconds] [workers] [fibers]
set -euo pipefail
BIN=$1 HOST=$2 PORT=$3 USER_=$4 PASS=$5 DB=$6 SEC=${7:-5} W=${8:-2} F=${9:-32}
for sc in point_reflect simple_select1 no_rows_one tx_begin_query_commit wide_2000_rows_named wide_200_rows_positional connect_close; do
  "$BIN" "$HOST" "$PORT" "$USER_" "$PASS" "$DB" "$SEC" "$W" "$F" "$sc" 2>/dev/null | tail -1
done
