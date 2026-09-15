# P6.1: prepare and inspect immutable paired RK/MCP releases

TKT-kogoj-lupun-gurab. Adds `release.prepare` / `release.list` / `release.show`
(RPC and `rk release prepare|list|show` CLI) — the first independent P6
delivery from `docs/2026-09-13-continuous-validation-promotion.md` sections 6
and 11. Native build routing (P4), activation, health recovery and rollback
(P7) are explicitly out of scope: preparation never touches the running
daemon, never stops it, and a prepared release is not "installed".

## User journey

```
rk repo add /path/to/rat-kingdom --name rk
rk release prepare --repo rk --candidate main
rk release list --repo rk
rk release show rel-<id>
```

`prepare` builds `rk-cli`/`rk-mcp` together from the exact resolved commit of
`--candidate` (a branch, tag, or sha) through one fixed recipe
(`paired-rk-mcp`), freezes the two binaries into an immutable,
content-hash-verified directory, and returns a versioned manifest. Calling
`prepare` again with the same `--repo`/`--candidate`/`--recipe` is a lookup,
not a rebuild: the release id is `rel-<sha256(repo, resolved commit, recipe,
recipe revision)>`.

## Source vs. prepared vs. installed

- **Source**: whatever `--candidate` resolves to in the registered repo's git
  history. Preparation freezes it as `source.resolved_commit`/`tree_sha` —
  the binding used for identity, never the mutable ref itself.
- **Prepared**: a release directory (`<home>/releases/<id>/`) holding
  `rk`, `rk-mcp`, and `manifest.json`. Nothing runs from here; nothing on the
  live daemon changed.
- **Installed**: the operator's existing `scripts/install.sh`/`mise run
  install` path, entirely unaffected by this ticket. Adopting a prepared
  release as the active installation is P7.

## Bounded recipe

`paired-rk-mcp` (the only recipe): `cargo build --release --target-dir
<owned dir> --jobs 2 -p rk-cli -p rk-mcp`, run inside a persistent per-repo
detached worktree (reset to the exact resolved commit on every call, keeping
`target/` warm across prepares), under `nice -n 10`, through `mise exec --`
when the resolved commit's tree carries a `mise.toml`/`.mise.toml`. Bounded to
20 minutes as one owned child process (reused kill/reap machinery from
`managed_verification.rs`). `--target-dir`/`CARGO_TARGET_DIR` are pinned
explicitly (both the CLI flag and the env var) so an inherited shared target
directory can never redirect this build's output.

Two bounded smoke checks follow, each with an isolated scratch `RK_HOME`
(never the daemon's production one, since the candidate source is arbitrary):
`rk --help` (clap's built-in help, no daemon connection) and a real MCP
`initialize` JSON-RPC request/response over `rk-mcp`'s stdin/stdout. These are
NOT the full `verify` check suite — the manifest's `checks` field and
`config_provenance.compatibility_checked: false` say so explicitly.

## Trust model

- Every binary hash and check result in a manifest was produced by this
  module's own execution — never accepted from a caller-supplied claim.
- A whole-manifest digest is committed to the durable registry entry
  BEFORE `manifest.json` is ever published (write-then-fsync-then-hard-link,
  never overwritten). Recovery after a crash between those two writes trusts
  ONLY that pre-committed digest — never a manifest file's own self-reported
  identity, since the release id is a hash of public inputs and therefore a
  guessable path.
- A `Prepared` registry entry whose `manifest.json` has gone missing is a
  rejected integrity failure, never a silent rebuild under the same identity.
- `release.show`/`release.prepare` recompute and report `content_verified`
  independently on every call.

## Config/compatibility limitations

- `config_provenance` records `used_mise`, the `.rk/repo.cue`/`.rk/checks.cue`
  content fingerprints at the resolved commit (each a `FileObservation`:
  present/absent/unavailable — a `git` observation failure is never silently
  treated as "absent"), and a best-effort `known_verification` reference to
  an existing exact-key managed-verification proof for the resolved commit
  (a pure read, never a check execution) when the daemon already holds one.
  `compatibility_checked` is always `false`: this slice's bounded smoke
  checks prove the binaries launch and speak their minimal protocol, not
  that they are compatible with any particular runtime/environment.
- `release.prepare` is operator-only (same authority shape as `repo.add`) and
  serialized by one process-wide lock — a simple single-flight guard, not the
  P3.1 `HostVerificationAdmission` aggregate cap.

## Cleanup and recovery

- A stale partial release directory (a prior attempt that wrote binaries but
  never completed a manifest) is moved to `<home>/releases-partial/<id>-<ts>/`
  on the next attempt at that id, never deleted — durable evidence stays
  inspectable. Broad garbage collection of that directory is out of scope.
- The persistent per-repo staging worktree (`<home>/release-staging/<repo>/`)
  is reused across prepares; nothing here tears it down automatically.
- A daemon crash mid-build leaves a `Preparing` registry entry with no live
  process behind it; `release.list`/`release.show` report it as `unknown`
  (via `Server::release_prepare_lock`'s `try_lock`), and a retried `prepare`
  with the same input safely resumes or rebuilds.

## Known follow-ups (not done here)

- `config_provenance.known_verification` is populated from a live,
  bounded lookup (`managed_verification::lookup_verification_proof`), but has
  no dedicated end-to-end test in this slice (it requires seeding a durable
  proof/landing-gate event matching the exact check identity) — covered only
  by the lower-level lookup function itself plus compilation of the wiring.
- No native release activation, rollback, or health supervision (P7).
