#!/usr/bin/env bash

CERTS_DIR=${CERTS_DIR:-/certs}
kwdb=$1
trap "set -x; killall kwbase kwbaseshort || true" EXIT HUP

set -euo pipefail

# Disable automatic network access by psql.
unset PGHOST
unset PGPORT
# Use root access.
export PGUSER=root

echo "Testing Unix socket connection via insecure server."
set -x

# Start an insecure CockroachDB server.
# We use a different port number from standard for an extra guarantee that
# "psql" is not going to find it.
"$kwdb" start-single-node --background --insecure \
              --socket-dir=/tmp \
              --listen-addr=:12345

# Wait for server ready.
"$kwdb" sql --insecure -e "select 1" -p 12345

# Verify that psql can connect to the server.
psql -h /tmp -p 12345 -c "select 1" | grep "1 row"

# It worked.
"$kwdb" quit --insecure -p 12345
sleep 1; killall -9 kwbase kwbaseshort || true

set +x
echo "Testing Unix socket connection via secure server."
set -x

# Restart the server in secure mode.
"$kwdb" start-single-node --background \
              --certs-dir="$CERTS_DIR" --socket-dir=/tmp \
              --listen-addr=:12345

# Wait for server ready; also create a user that can log in.
"$kwdb" sql --certs-dir="$CERTS_DIR" -e "create user foo with password 'pass'" -p 12345

# Also verify that psql can connect to the server.
env PGPASSWORD=pass psql -U foo -h /tmp -p 12345 -c "select 1" | grep "1 row"

set +x
# Done.
"$kwdb" quit --certs-dir="$CERTS_DIR" -p 12345
