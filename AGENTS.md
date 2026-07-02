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

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:970c3bf2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
