#!/bin/bash
# Hermes monitor_script for hermes-webhook-relay: copy to ~/.hermes/scripts/.
#
# Pulls new events into the inbox, then lists what is pending. Hermes runs the
# job's agent only when this output differs from the last run, and alerts when
# the script exits non-zero (the queue or the inbox is unreachable).
set -euo pipefail

relay="${RELAY_CONSUMER:-$HOME/.local/bin/relay-consumer}"

# Hermes compares stdout byte for byte, and pull's counts change every run.
"$relay" pull >&2
"$relay" pending
