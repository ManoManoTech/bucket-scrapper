# Contributing

See [Contributing to this repository](https://github.com/ManoManoTech/ALaMano/blob/master/CONTRIBUTING.md).

## Git hooks

This repo ships a [lefthook](https://github.com/evilmartians/lefthook) config (`lefthook.yml`) that mirrors CI:

- `pre-commit`: `cargo fmt --check`
- `pre-push`: `cargo clippy --all-targets -- -D warnings`

Lefthook is pinned in `mise.toml`. After cloning, run `mise install` then `mise exec -- lefthook install` once to wire `.git/hooks/`. See `AGENTS.md` for why every dev tool lives in `mise.toml`.
