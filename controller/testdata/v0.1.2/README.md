# Released-writer journal fixtures

The seven legacy JSON journals were written by the actual `v0.1.2` controller and
`echoagent.Harness`, at commit `adb5d10e56f0eeb3a128e669218942a9c25f7f39`.
Each file contains the original `eventlog.Record` envelopes and events, including
sequence numbers, fences, previous hashes and content hashes. Tests verify those
hashes on the current code, copy the records into SQLite and reopen the database.
No current-writer events were stripped of execution IDs or start markers.

`generate.go` interrupts the final invocation by panicking from `OnRecord` after
a successful append. This simulates process death without writing an `ERROR` or
`END`. The earlier invocation in multi-turn fixtures commits normally.
`model-error.json` instead makes the model return an error during the tagged
controller's `Exec`, so that writer itself persists `INPUT, MODEL_CALL, ERROR`.

| Fixture | Journal boundary |
| --- | --- |
| `completed.json` | One completed echo turn. |
| `crashed-input.json` | After INPUT, before the model runs. |
| `crashed-model.json` | After MODEL_CALL, before OUTPUT; recovery still rejects the missing completion. |
| `model-error.json` | Model failure ends the legacy turn with ERROR; Resume is a completed no-op. |
| `crashed-output.json` | After OUTPUT, before END; recovery must serve the completion. |
| `multi-turn.json` | Two completed turns with distinct inputs. |
| `multi-turn-crashed-output.json` | A completed first turn and a second turn interrupted after OUTPUT. |
| `upgraded-resume.json` | Current Resume completes `crashed-output.json`, retaining ID-less events. |
| `mixed-completed.json` | The upgraded recovery followed by a completed current-writer Exec. |
| `mixed-interrupted.json` | The upgraded recovery followed by a current-writer Exec interrupted after OUTPUT. |

## Generate

Run the legacy generator **from the tagged module**, passing the absolute path to
the generator and destination in the current checkout:

```sh
git worktree add --detach /tmp/agentsessions-v012 v0.1.2
cd /tmp/agentsessions-v012
go run /path/to/current/controller/testdata/v0.1.2/generate.go \
  /path/to/current/controller/testdata/v0.1.2
cd /path/to/current
go run controller/testdata/v0.1.2/generate-mixed.go controller/testdata/v0.1.2
```

The mixed generator runs on the current module and preserves the original crashed
prefix before Resume and Exec. Model-call and modern execution IDs are random,
so regeneration produces different hashes; the committed files are frozen inputs.
To generate only the new failure fixture without changing any older random IDs,
run the legacy command with a final `model-error` argument. Timestamps, execution
IDs, fences, and record envelopes are emitted by the tag writer, not rewritten.

## Deliberately retained limitations

The released Replay called the harness once with all inputs and the whole journal
as History, not once per inferred turn. Thus `multi-turn.json` fails echo replay
with the historical model-input hash mismatch: echo selects the last input but
the first recorded model call used the first input. This is not a missing-ID
rejection. Inferring turn boundaries would change historical behavior.

A new `EXECUTION_START` explicitly supersedes an unfinished or failed legacy tail.
Mixed replay omits that abandoned tail, retaining at most the prefix through the
last legacy `END` as one whole-log Run. Modern invocations still receive the full
original prefix as History, including the abandoned tail. No old records or hashes
are rewritten, and pure-legacy Replay retains its historical behavior.

Resume retains the released last-INPUT rule, so the second interrupted turn in
`multi-turn-crashed-output.json` can be recovered without repeating its model
call. Config, resume cursor and expected input count were not persisted by this
writer and cannot be reconstructed retroactively. Output-delta execution IDs
were ephemeral and never part of these journals.
