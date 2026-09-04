# Contributing

Thanks for your interest. This project is pre-1.0 and the contract is still moving, so open an issue
before starting anything large.

## Build and test

```bash
go build ./...
go test -race ./...
```

`integrations/substrate` is a separate Go module and is not reached by `./...` from the root. If you
touch it, test it on its own:

```bash
cd integrations/substrate && go test -race ./...
```

## What CI checks

Run these before pushing; they are the same gates the `ci` workflow enforces.

| Gate | Command |
|---|---|
| Formatting | `gofmt -l .` must print nothing |
| Build and vet | `go build ./...`, `go vet ./...` |
| Tests | `go test -race ./...` |
| Vendor neutrality | `go list -deps ./... \| grep -c agent-substrate` must be `0` |
| Provenance self-check | `python3 hack/verify_chain.py --golden` |
| Proto lint | `buf lint` |

Two gates catch stale generated files, and both fail the build rather than regenerating for you:

```bash
buf generate                 # api/genpb must not change afterwards
./hack/gen-api-reference.sh  # docs/api-reference.md must not change afterwards
```

Run both after any change to `api/*.proto`.

`buf breaking` compares the schema against the most recent release tag. There is no tag yet, so it
currently skips. It arms itself at the first release.

## Conventions

- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/). Keep the
  description to one imperative line.
- **The core stays vendor-neutral.** No cloud, sandbox, or provider-specific code or naming in the
  root module. Concrete backends live in their own modules or repositories, which is what the vendor
  neutrality gate enforces.
- **Go examples in docs get compile-checked** before merging. A snippet that does not build is worse
  than no snippet, and several already sit directly on top of SPIs that are still changing.
- **Docs land with the code they describe**, not ahead of it. If a change alters an interface that a
  document shows, update the document in the same pull request.

## Governance

Decisions rest with the maintainer, [@aramase](https://github.com/aramase). There is no wider
governance structure yet because there is not yet a wider group of contributors; that will change if
and when it needs to.

## Reporting security issues

Do not open a public issue. See [SECURITY.md](SECURITY.md).
