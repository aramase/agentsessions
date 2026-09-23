#!/usr/bin/env bash
# The full tour, driven against a real Kubernetes deployment rather than a local toy.
#
# Covers every capability the project actually ships: exec, multi-turn, replay with no provider
# call, fork into independent children, suspend and resume, listing, and finally pod death with
# the session surviving it.
#
# Requires: a cluster, kubectl, agentctl on PATH, and deploy/kubernetes/agentsessionsd.yaml applied.
set -euo pipefail

NS=agentsessions
PORT=${PORT:-18080}
SERVER=127.0.0.1:${PORT}

c_head=$'\033[1;36m'
c_cmd=$'\033[1;32m'
c_dim=$'\033[0;90m'
c_off=$'\033[0m'

say() {
	printf '\n%s# %s%s\n' "$c_head" "$*" "$c_off"
	sleep 1.4
}
note() {
	printf '%s  %s%s\n' "$c_dim" "$*" "$c_off"
	sleep 1
}
# Pass the command single-quoted so $SERVER and $SID print as names and expand only on eval.
run() {
	printf '%s$%s %s\n' "$c_cmd" "$c_off" "$1"
	sleep 0.7
	eval "$1"
	sleep 1.3
}

cleanup() { [ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null || true; }
trap cleanup EXIT

if lsof -ti "tcp:${PORT}" >/dev/null 2>&1; then
	echo "port ${PORT} is already in use; set PORT=<free port> or stop the other listener" >&2
	exit 1
fi

forward() {
	[ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null && sleep 1
	kubectl -n "$NS" port-forward svc/agentsessionsd "${PORT}:8080" >/dev/null 2>&1 &
	PF_PID=$!
	for _ in $(seq 20); do
		nc -z 127.0.0.1 "$PORT" 2>/dev/null && return 0
		sleep 0.5
	done
	echo "port-forward to ${NS}/agentsessionsd never became ready" >&2
	exit 1
}

exec 2>/dev/null

forward

say "agentsessionsd on a plain Kubernetes Deployment. One pod, one PersistentVolume."
run 'kubectl -n $NS get deploy,pvc,pod -l app=agentsessionsd -o name'
note "SERVER=$SERVER, forwarded from svc/agentsessionsd:8080"

say "1. Run a turn."
OUT=$(agentctl exec --server "$SERVER" --input "plan a trip to Tokyo" 2>/dev/null)
printf '%s$%s agentctl exec --input "plan a trip to Tokyo"\n' "$c_cmd" "$c_off"
sleep 0.7
echo "$OUT"
SID=$(echo "$OUT" | head -1 | cut -d' ' -f2)
note "four typed records: the input, the host-mediated model call, the output, the end"
sleep 1.2

say "2. Keep talking. The sequence keeps climbing in one log."
run 'agentctl exec --server $SERVER --session $SID --input "make it a week"'

say "3. Replay the whole session. The harness never runs; the provider is never called."
run 'agentctl replay --server $SERVER --session $SID'
note "identical bytes, served from the journal"

say "4. Fork it. Two children branch from seq 4, at no model cost."
FORKED=$(agentctl fork --server "$SERVER" --session "$SID" --at 4 --count 2 --names "budget,luxury" 2>/dev/null)
printf '%s$%s agentctl fork --session %s --at 4 --count 2 --names "budget,luxury"\n' "$c_cmd" "$c_off" "${SID:0:18}"
sleep 0.7
echo "$FORKED"
# Reuse the first named child rather than forking a third one nobody saw being created.
CHILD=$(echo "$FORKED" | head -1 | cut -d' ' -f2)
sleep 1.3

say "5. A child inherits the prefix, then diverges on its own."
run 'agentctl replay --server $SERVER --session $CHILD | tail -3'
run 'agentctl exec --server $SERVER --session $CHILD --input "child-only turn" | tail -4'
note "the parent is untouched; each branch owns its own future"

say "6. Suspend releases compute. Resume brings it back."
run 'agentctl suspend --server $SERVER --session $SID'
run 'agentctl resume --server $SERVER --session $SID'

say "7. Sessions are listable, with or without compute."
run 'agentctl list --server $SERVER | head -5'

say "8. Now delete the pod serving all of this."
run 'kubectl -n $NS delete pod -l app=agentsessionsd'
run 'kubectl -n $NS rollout status deploy/agentsessionsd --timeout=120s'

forward

say "9. A different pod. Replay a session it never ran."
run 'agentctl replay --server $SERVER --session $SID | tail -4'

say "10. And continue it, in the same log, from where it stopped."
run 'agentctl exec --server $SERVER --session $SID --input "still here after pod death" | tail -4'

say "The pod was cattle. The session was not."
sleep 2.5
