# AGENTS.md

Operational rules for AI agents (and humans) working in this repo. Read before making any toolchain or dependency change.

## Tool management: mise is the source of truth

**If mise can manage it, mise must manage it.** No exceptions for "just this once" or "the user already has it installed." This is the single most important rule in this file.

That means:

- Any binary tool used in dev, test, or build — language toolchains (rust, node, python), CLIs (lefthook, samply), task runners, formatters, linters — goes in `mise.toml`. Never document a tool with `brew install`, `apt install`, `go install`, `npm install -g`, `cargo install`, `pipx install`, or a `curl | sh` one-liner without first checking `mise registry | grep <tool>`.
- `mise registry` lists everything mise can install. If a tool is listed there, the only acceptable way to introduce it is by adding it to `mise.toml` and committing the change. Tell contributors to run `mise install`, not to install the tool a second way.
- If a tool is **not** in the mise registry, that's the only situation where an alternative install path is acceptable. Document it explicitly with a one-line justification ("not in mise registry as of YYYY-MM-DD").
- Never edit `~/.tool-versions`, `.envrc`, shell rc files, or per-user paths to make a tool available — those are invisible to teammates and CI. Use `mise.toml`.

Why: every tool that's installed out-of-band creates a "works on my machine" footgun. Lint drift, version skew, hook silently not running because someone has an older binary — all of these trace back to a tool that should have been pinned in `mise.toml` and wasn't. Treat `mise.toml` as a hard contract: if it's not declared there, contributors and CI cannot rely on it being present.

How to apply when in doubt: before suggesting any install command, run `mise registry | grep -i <tool>`. If it matches, edit `mise.toml`. If it doesn't, say so explicitly in the change description.

## Git hooks

`lefthook.yml` mirrors CI (fmt on pre-commit, clippy on pre-push). Lefthook itself is pinned in `mise.toml`. After `mise install`, run `mise exec -- lefthook install` once to wire `.git/hooks/`. Don't bypass hooks (`--no-verify`); fix the underlying issue.
