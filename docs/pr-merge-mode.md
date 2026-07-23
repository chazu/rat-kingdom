# PR / MR merge mode — operator guide

By default a finished rat's branch is **merged directly** into its base by the
daemon (`rk dismiss` / a workflow `dismiss`/`land` step → a local `git merge`).
For projects that want a human or CI review *before* code lands, a repo can be
put in **PR mode**: instead of merging, the daemon pushes the rat's branch and
opens a pull/merge request, leaving the branch standing for review.

This guide covers the prerequisites, how to switch a repo into PR mode, what the
GitHub vs GitLab flows look like, and the end-to-end review path.

> Design background and the seams this rides on are in
> [`docs/proposals/pr-merge-mode.md`](proposals/pr-merge-mode.md). This page is
> the operator-facing how-to for the shipped feature (TKT-63…65).

---

## 1. How it works (one paragraph)

Every merge in the system funnels through one seam: `Supervisor::dismiss` (a
rat's branch → its own base) and `Supervisor::land` (an explicit
`{branch, target}`). PR mode branches that seam on the repo's **`merge_mode`**:
`direct` runs the historical local `git merge`; `pr` runs
`git push` + open-PR-**via-git-only** (no `gh`/`glab` dependency — an operator
decision), then **does not merge and does not delete the branch**. The result
carries `{merged: false, pr_opened: true, pr_url}` and the daemon emits a
`pull_request_opened` event alongside the usual `agent_dismissed`/`branch_landed`.

There is **no separate auth surface**: the push uses the repo checkout's own
already-configured git credentials, exactly as if you ran `git push` in it
yourself.

---

## 2. Prerequisites — git credentials for the daemon user

PR mode pushes to a remote. That push runs **in the daemon's process, under the
daemon user's environment** — not in your interactive shell, and not in a rat's
throwaway worktree. So the credentials that authorize the push must be reachable
by the daemon user. Before switching a repo to `pr`, confirm all of:

1. **The repo has a remote.** `git -C <repo> remote -v` shows an `origin` (or
   whichever remote you intend to use) pointing at the GitHub/GitLab host. The
   host is inferred from this URL at registration time.

2. **The daemon user can push to it non-interactively.** The push must succeed
   with **no prompt** — a stuck credential prompt in a background daemon becomes
   a clean `pr_opened: false` (the push failed), not a hang, but also not a PR.
   Options, in rough order of preference:
   - An **SSH remote** (`git@github.com:owner/repo.git`) with the daemon user's
     SSH key loaded / in an agent the daemon can reach.
   - A **credential helper** already primed for the daemon user
     (`git config --global credential.helper …`, e.g. the OS keychain or a
     `store` file the daemon user owns).
   - A token baked into an HTTPS remote URL (least preferred — it lands in the
     repo config in plaintext).

3. **Push permission on the branch namespace.** The daemon pushes
   `rat/<agent>/<task>` branches. The remote must allow the daemon user to
   create branches under that namespace (watch for protected-branch or
   push-rule restrictions that would reject `rat/*`).

Quick self-test — run this **as the daemon user, from the registered checkout**,
and it must succeed with no prompt:

```bash
git -C <repo> push -u origin HEAD:rat/smoketest/cred-check
git -C <repo> push origin --delete rat/smoketest/cred-check   # clean up
```

If that prompts or fails, PR mode will report `pr_opened: false` — fix the
credentials first.

---

## 3. Configuring a repo for PR mode

### Per-repo (recommended)

Set the mode when you register the repo, or re-register to change it:

```bash
rk repo add ~/dev/svc --merge-mode pr                 # open a PR instead of merging
rk repo add ~/dev/svc --merge-mode pr --remote upstream # push/PR against a non-origin remote
rk repo add ~/dev/svc --merge-mode direct             # the default; explicit here
```

- `--merge-mode` accepts `direct` or `pr`. **Omit it** to inherit the
  fleet-wide default (see below).
- `--remote` chooses which remote to push / open the PR against (defaults to
  `origin`).
- The **host** (`github.com`, `gitlab.com`, …) is inferred from that remote's
  URL at registration and stored on the repo record — it decides GitHub vs
  GitLab behavior (§4).

Inspect what got recorded:

```bash
rk repo show svc
#   merge      pr
#   remote     origin
#   host       github.com
```

`rk repo list` also shows each repo's mode.

### Fleet-wide default

A repo registered **without** `--merge-mode` falls back to the daemon's
`[policy] default_merge_mode` in `~/.rat-kingdom/config.toml`:

```toml
[policy]
default_merge_mode = "direct"    # or "pr" — fleet-wide fallback for repos
                                 # registered without an explicit --merge-mode.
                                 # A repo's own --merge-mode always overrides this.
```

Default is `direct`, so the feature is fully opt-in and backward compatible: a
registry file written before PR mode existed, and any repo added without the
flag, behaves exactly as before.

---

## 4. GitHub vs GitLab — the two flows

The daemon opens the PR **over plain `git`**, so the mechanism differs by host
(inferred from the remote URL):

### GitLab — MR created for you (push options)

For a GitLab host the daemon pushes with merge-request **push options**:

```
git push -o merge_request.create -o merge_request.target=<base> <remote> <branch>
```

GitLab creates the merge request server-side as part of the push, targeting the
rat's base branch. The **MR URL** comes back on the push's stderr and is
captured into `pr_url` / the `pull_request_opened` event. Nothing else to do —
the MR exists.

### GitHub — branch pushed, compare URL surfaced

GitHub has no create-PR-via-push, so the daemon just pushes the branch:

```
git push -u <remote> <branch>
```

GitHub prints a **compare URL** (`https://github.com/owner/repo/pull/new/<branch>`)
on stderr, which is captured into `pr_url`. A human clicks it to actually open
the PR. So on GitHub the daemon gets you one click away — the branch is up and
the compare link is waiting in the inbox/event; it does not open the PR for you.

### Unknown / self-hosted hosts

Any host that isn't recognized as GitHub or GitLab is treated like GitHub: the
branch is pushed and whatever URL the remote prints is surfaced. No MR is
created for you; open it manually.

### Failure is clean, never a crash

A push/auth/remote failure is a `pr_opened: false` with an explanatory `detail`
(mirroring how a merge conflict is a clean `merged: false`) — the daemon never
panics, and the branch is left intact for you to retry.

---

## 5. End-to-end review path

Direct mode and PR mode differ only in **what "proceed" does** — every gate that
decides *whether* to proceed (approval gates, the steward's reviewer + run-gate)
is unchanged.

**Direct mode (default):**

```
rat finishes → rk dismiss / dismiss step
             → local git merge into base → branch deleted → ticket closed
```

**PR mode:**

```
rat finishes → rk dismiss / dismiss step
             → git push (+ MR on GitLab) → branch KEPT, base untouched
             → pull_request_opened event (pr_url)
             → human/CI reviews and merges the PR on the host
```

Key differences to expect in PR mode:

- **The branch survives.** In direct mode the daemon deletes the merged branch;
  in PR mode the PR owns the branch, so it is left standing. Cleanup after the
  PR merges is the host's job (e.g. delete-branch-on-merge), not the daemon's.
- **The base is untouched** until a human/CI merges the PR — that is the whole
  point.
- **`merged` is `false`.** The result shape is
  `{merged: false, pr_opened: true, pr_url: "…"}`. A workflow that asserts
  `evaluate {expect: {merged: true}}` after a `dismiss`/`land` will **not** see
  `merged: true` in PR mode — that gate-outcome adaptation (asserting
  `pr_opened` instead) and an "awaiting-review" inbox source are tracked
  separately (TKT-66/67); until they land, drive PR-mode repos with plain
  `rk dismiss` / gates that don't hard-assert `merged`.
- **The ticket is not auto-closed** on dismiss, because nothing merged — it
  closes when the branch actually lands via the PR.

### Watching for opened PRs

The `pull_request_opened` event carries `{agent, branch, target, url, detail}`.
Until the dedicated inbox row lands (TKT-67), you can watch the event stream or
read the URL off the `rk dismiss` result / the agent's log to find the PR to
review.

---

## 6. Quick reference

```bash
# switch a repo to PR mode (push + open PR instead of merging)
rk repo add <path> --merge-mode pr [--remote <name>]
rk repo show <name>            # merge / remote / host as recorded

# fleet-wide default (config.toml)
[policy]
default_merge_mode = "pr"      # or "direct" (the default)
```

| | Direct mode | PR mode |
|---|---|---|
| action on dismiss/land | local `git merge` | `git push` + open PR |
| base branch | advanced | untouched until PR merges |
| rat's branch | deleted on merge | kept for review |
| result | `merged: true` | `merged: false, pr_opened: true, pr_url` |
| event | `agent_dismissed` | `agent_dismissed` + `pull_request_opened` |
| GitHub | — | branch pushed, compare URL surfaced |
| GitLab | — | MR created via push option |
| auth | none | repo checkout's own git credentials |
