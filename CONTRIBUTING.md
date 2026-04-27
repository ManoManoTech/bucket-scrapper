# Contributing

See [Contributing to this repository](https://github.com/ManoManoTech/ALaMano/blob/master/CONTRIBUTING.md).

## Git hooks

This repo ships a [lefthook](https://github.com/evilmartians/lefthook) config (`lefthook.yml`) that mirrors CI:

- `pre-commit`: `cargo fmt --check`
- `pre-push`: `cargo clippy --all-targets -- -D warnings`

Install lefthook once (`brew install lefthook` / `go install github.com/evilmartians/lefthook@latest`) then run `lefthook install` in the repo to wire up `.git/hooks/`.
