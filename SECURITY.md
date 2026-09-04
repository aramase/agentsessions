# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/aramase/agentsessions/security/advisories/new)
rather than opening a public issue.

Include what you were running, what you observed, and how to reproduce it. You should get an initial
response within a week.

## Supported versions

None yet. This project has not had a release, so there is no supported version and no backport
branch. Fixes land on `main`.

## What is in scope

The interesting surface is the durable log and its provenance chain. Reports that show any of the
following are especially useful:

- A way to append to a session log while holding a superseded fencing token.
- A way to break the compare-and-swap on append so two writers both commit.
- A hash chain that still verifies after a record is altered, removed, or reordered.
- A divergence between the Go hash and the canonical form the Python verifier reproduces, since the
  chain is only auditable by third parties if those agree.
- Replay invoking a model provider, or otherwise reproducing a session non-deterministically.

## What is not in scope

The reference implementation has no authentication, no authorization, and no transport security.
Every gRPC path is plaintext, `IdentityRef` is carried but never enforced, and `project` is a filter
rather than a tenancy boundary. A host is not safe to expose to an untrusted network or to treat as
multi-tenant.

That is a known state rather than an oversight, so reports that a host can be reached by an
unauthenticated caller, or that one project can read another, are documented behaviour and not
vulnerabilities. If you find something that is still exploitable once those are accounted for, please
do report it.
