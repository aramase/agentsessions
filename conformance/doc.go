// Package conformance is the replay-conformance suite: the CSI/CRI-style neutral checks that any
// host/backend/harness combination must pass, run here against the REAL stack (the persistent
// sqlitelog backend + the controller + the echo harness). It deliberately exercises what the pod
// demo did not — genuine model nondeterminism, multi-turn resume-then-continue across incarnations,
// crash-mid-turn recovery (at-most-once), fork equivalence, single-writer, and language-neutral
// integrity — turning "it ran once" into "the determinism contract holds". See the determinism
// contract §9.
package conformance
