//! TKT-jonis-faror-zufuj, NATIVE lifecycle proof: a generation resumed in
//! place — while its FIRST delivery is still held pending inside native
//! landing — delivers a second, later commit to the same branch/target, and
//! `finalize_delivery` settles that successor instead of busy-retrying a
//! merge-pointer conflict forever.
//!
//! This is the companion the pipeline-seam test in
//! `crates/rk-daemon/src/landing.rs`
//! (`successor_and_stale_replay_settle_through_the_pipeline_seam`) explicitly
//! does NOT provide. Nothing here fabricates an `AgentRecord`, hand-rolls the
//! second source with raw `git`, advances the target itself, calls a pipeline
//! method directly, or stands a `tokio::spawn`ed `Daemon::run()` in for a
//! daemon process:
//!
//! 1. A real fake-harness rat is dispatched over the wire (`rk spawn`),
//!    commits its first source, and declares `rk done` through the real `rk`
//!    binary. The live reactor's `action: "land"` trigger is what enqueues it.
//! 2. That first delivery is held PENDING inside native landing: its diff is
//!    NOT doc-only, so `LandingPipeline` routes it to a native reviewer, and
//!    that reviewer blocks on a per-head release file.
//! 3. While it is still held, the SAME generation is resumed through the real
//!    `rk respawn` path — same record, same `SpawnId`, same branch, same
//!    worktree, same cost ledger. Its second launch commits the successor
//!    source and declares done again, which the reactor enqueues as a second
//!    candidate under the identical `source_spawn`. This is the ticket's
//!    CORRECTED ordering: the resume happens BEFORE the first delivery lands,
//!    which is why the guard against resuming a generation whose merge
//!    pointer is already canonically landed is never relevant here and is not
//!    relaxed.
//! 4. The first delivery is then released and lands. Native review — a
//!    spawned reviewer generation writing its own verdict artifact under the
//!    daemon's `RK_REVIEW_*` binding, not a doc-only bypass and not a verdict
//!    the test injected — is what permits the successor too.
//! 5. The successor's advance is interrupted at exactly the incident's point.
//!    `crate::fault`'s `landing-post-target-advance` barrier parks daemon A
//!    *inside* the window where the target has moved but `finalize_landed`
//!    has not run, and announces it with a file; only then is daemon A
//!    SIGKILLed as a real OS process. No sleep stands in for that window, and
//!    nothing is hand-cleaned afterwards: daemon B is auto-started by the
//!    next `rk` invocation's `connect_or_spawn`, exactly as in the field, and
//!    it is that genuinely new process which recovers the durable `Landing`
//!    receipt and finalizes it.
//! 6. Daemon B is then stopped and daemon C — a third real process — comes up
//!    over the same home to prove the replay is idempotent: the target does
//!    not advance again, the merge pointer does not move again, exactly one
//!    `delivery_merge_pointer_advanced` event exists, and the generation's
//!    recorded cost does not grow.
//!
//! HONEST LIMITATIONS. (a) The harness is `fake`, whose `caps().resume` is
//! `false`, so no provider session is literally resumed — what is exercised
//! is the production `rk respawn`/`respawn_generation` path continuing the
//! same `SpawnId`, record, branch, worktree and cost ledger, which is the
//! part the delivery seam can actually observe. A paid-provider session
//! resume is out of scope for a disposable-repo fixture. (b) `rk respawn`
//! takes no `--spawn` pin (the RPC does), so the exact-generation check here
//! is an assertion on the record's `spawn` before and after, not a refusal
//! the daemon made. (c) The landing gates are trivially-passing named checks:
//! this fixture is about the delivery seam, not about check content.
//!
//! Process/teardown discipline and the `rk`/`until`/`kill_owning_daemon`
//! helpers follow `crates/rk-cli/tests/review_ceiling_crash_barrier.rs`,
//! which does the same thing for the review-ceiling window; home and repo
//! setup follow `crates/rk-cli/tests/daemon_rollover.rs`.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// Must match `BARRIER_POST_TARGET_ADVANCE` in
/// `crates/rk-daemon/src/landing.rs`. The constant is private to that module;
/// a barrier's contract is the name string, deliberately, so arming needs no
/// API surface (same as `review_ceiling_crash_barrier.rs`).
const BARRIER_POST_TARGET_ADVANCE: &str = "landing-post-target-advance";

/// The daemon-native landing pipeline's completion feed, reproduced here
/// rather than read from `examples/triggers-landing-pipeline.cue` so this
/// test does not depend on that example's path or content staying
/// byte-identical (same reasoning as `live_landing_restart.rs`).
const LANDING_TRIGGER: &str = r#"
triggers: [
    {
        name:   "resumed-generation-landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

/// A `candidate-review` definition shaped like the shipped
/// `examples/workflows/candidate-review.cue` — same params, and above all the
/// same `review:` binding on the spawn. That binding is what gives the
/// reviewer its `RK_REVIEW_*` env, and therefore what lets it record a
/// verdict artifact the server's `validate_review_artifact` will accept — on
/// the `fake` harness, so no tokens are spent.
const REVIEW_WORKFLOW: &str = r#"
package workflow
workflow: {
    name: "candidate-review"
    params: {
        taskId: {type: "string", required: false, default: "unknown"}
        branch: {type: "string", required: true}
        repo: {type: "string", required: false, default: "unknown"}
        target: {type: "string", required: false, default: "main"}
        headSha: {type: "string", required: false, default: ""}
        reviewAttempt: {type: "string", required: true}
        reviewTimeout: {type: "string", required: false, default: "10m"}
    }
    agents: {default: {harness: "fake"}, reviewer: {harness: "fake"}}
    steps: [
        {
            type:   "spawn"
            role:   "reviewer"
            agent:  "reviewer"
            branch: _input.branch
            review: {
                branch:  _input.branch
                headSha: _input.headSha
                target:  _input.target
                task:    _input.taskId
                attempt: _input.reviewAttempt
            }
            task: {
                title:       "candidate-review-" + _input.taskId
                description: "review \(_input.branch) at \(_input.headSha) for \(_input.target)"
            }
        },
        {type: "wait", timeout: _input.reviewTimeout},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

/// The fake harness both roles run.
///
/// The rat counts its own launches in `<control>/rat-launches`, so the SECOND
/// launch — the resumed generation — commits a genuinely different source
/// than the first. 60 lines in one non-doc file puts the candidate over
/// `DIFF_TRIVIAL_MAX_LINES` (`crates/rk-daemon/src/supervisor.rs`), which is
/// what keeps it out of the `doc-only | trivial` fast path and forces a real
/// native review.
///
/// The reviewer blocks on `<control>/release-$RK_REVIEW_HEAD` — PER HEAD, not
/// one shared gate, which is what makes the two deliveries individually
/// releasable and the whole fixture deterministic: the first delivery can be
/// finalized and observed before the successor's reviewer is even allowed to
/// return. It then records its OWN verdict; `rk out artifact` stamps the
/// binding fields (branch/head_sha/target/task/review_attempt) from the
/// `RK_REVIEW_*` env the daemon set on this spawn
/// (`crates/rk-cli/src/space_cmds.rs`), so the verdict is bound to the exact
/// candidate under review. Both roles declare `rk done` before their result
/// line, as a real primed rat does (TKT-175), and the reviewer declares it
/// before writing the verdict so the pipeline reacting to that artifact
/// cannot tear the generation down with its `task_done` still unwritten.
fn fake_harness(rk_bin: &str, control: &Path) -> String {
    let control = control.display();
    format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"resumed-gen-fake"}}'
if [ "$RK_ROLE" = "reviewer" ]; then
    i=0
    while [ ! -f "{control}/release-$RK_REVIEW_HEAD" ] && [ "$i" -lt 6000 ]; do
        sleep 0.05
        i=$((i+1))
    done
    "{rk_bin}" done "review complete" >/dev/null 2>&1
    "{rk_bin}" out artifact "$RK_REPO" review --payload '{{"recommendation":"APPROVE","notes":"native reviewer approved this candidate"}}' >/dev/null 2>&1
    echo '{{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"resumed-gen-fake","total_cost_usd":0.002,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
else
    n=$(cat "{control}/rat-launches" 2>/dev/null || echo 0)
    n=$((n+1))
    printf '%s' "$n" > "{control}/rat-launches"
    : > src_gen.rs
    j=1
    while [ "$j" -le 60 ]; do
        echo "pub const SOURCE_V${{n}}_${{j}}: u32 = $j;" >> src_gen.rs
        j=$((j+1))
    done
    git add src_gen.rs >/dev/null 2>&1
    git -c user.email=r@x -c user.name=R commit -q -m "feat: source v$n"
    "{rk_bin}" done "source v$n committed" >/dev/null 2>&1
    echo '{{"type":"result","subtype":"success","is_error":false,"result":"source committed","session_id":"resumed-gen-fake","total_cost_usd":0.01,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
fi
"#
    )
}

/// An `rk` invocation driven as the operator. The env set here reaches the
/// daemon too: `connect_or_spawn`'s detached daemon spawn inherits this
/// process's environment, so whichever `rk` call happens to auto-start a
/// daemon hands it `RK_FAKE_HARNESS_CMD` — which is why EVERY call in this
/// test goes through here (`daemon_rollover.rs` documents the same trap).
fn rk(home: &Path, control: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    cmd.env(
        "RK_FAKE_HARNESS_CMD",
        fake_harness(env!("CARGO_BIN_EXE_rk"), control),
    );
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn is_ancestor(dir: &Path, ancestor: &str, descendant: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .unwrap()
        .success()
}

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "rk failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "bad json: {e}\nstdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Poll `attempt` until it yields `Some`, or panic with `what` after 90s.
/// Every wait in this test is a wait on an *observed condition* — never a
/// bare sleep standing in for one.
fn until<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(90) {
        if let Some(value) = attempt() {
            return value;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out after 90s waiting for: {what}");
}

/// Kill whichever daemon currently owns `home` — teardown only, best effort.
/// Reads the pid file directly rather than round-tripping an RPC, for the
/// reason `review_ceiling_crash_barrier.rs` spells out: a daemon does not
/// exit on its own, so a missed kill here is a permanent leak.
fn kill_owning_daemon(home: &Path) {
    let Some(pid) = std::fs::read_to_string(home.join("rk.pid"))
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
    else {
        return;
    };
    if pid == std::process::id() {
        return;
    }
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// RAII teardown for the real detached daemons this test starts. Declared
/// AFTER the `TempDir` it guards so it drops BEFORE that `TempDir`'s
/// destructor removes the directory (locals drop in reverse declaration
/// order): the pid file must still exist when [`kill_owning_daemon`] reads
/// it. Runs on panic as well as success, so a failed assertion cannot leak a
/// daemon into the host running the suite.
struct DaemonGuard {
    home: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        kill_owning_daemon(&self.home);
    }
}

/// Bring a daemon up (if none owns `home`) and return its pid. `rk ping` is
/// what triggers `connect_or_spawn`; `rk daemon status` then reports the pid
/// of whichever process actually owns the socket.
fn daemon_pid(home: &Path, control: &Path) -> u32 {
    let ping = rk(home, control).args(["ping"]).output().unwrap();
    assert!(
        ping.status.success(),
        "rk ping: {}",
        String::from_utf8_lossy(&ping.stderr)
    );
    json_stdout(
        &rk(home, control)
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .expect("daemon status must report a pid") as u32
}

fn agent_record(home: &Path, control: &Path, name: &str) -> Value {
    json_stdout(
        &rk(home, control)
            .args(["--json", "status", name])
            .output()
            .unwrap(),
    )
}

fn scan(home: &Path, control: &Path, scope: &str, identity: &str) -> Vec<Value> {
    json_stdout(
        &rk(home, control)
            .args(["--json", "scan", "event", scope, identity])
            .output()
            .unwrap(),
    )["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Durable landing-queue rows, read purely through `rk scan` — the operator's
/// own surface, never `rk-daemon`'s internal `LandingQueue`.
fn queue_rows(home: &Path, control: &Path, scope: &str) -> Vec<Value> {
    scan(home, control, scope, "landing_queue_entry")
        .into_iter()
        .map(|tuple| tuple["payload"].clone())
        .collect()
}

fn ticket_delivery(home: &Path, control: &Path, task: &str) -> Option<String> {
    json_stdout(
        &rk(home, control)
            .args(["--json", "ticket", "show", task])
            .output()
            .unwrap(),
    )["payload"]["delivery"]["merge_commit"]
        .as_str()
        .map(str::to_string)
}

/// A daemon home with the landing trigger and the fake-harness review
/// workflow installed, and the disk-pressure floor disabled (a constrained CI
/// temp filesystem would otherwise refuse every spawn before this test
/// reached anything it means to cover — same reasoning as
/// `daemon_rollover.rs`).
fn daemon_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        "[disk]\nmin_free_gb = 0\n\n[harness]\ndefault = \"fake\"\n",
    )
    .unwrap();
    let workflows = home.path().join("workflows");
    std::fs::create_dir_all(&workflows).unwrap();
    std::fs::write(workflows.join("candidate-review.cue"), REVIEW_WORKFLOW).unwrap();
    let triggers = home.path().join("triggers");
    std::fs::create_dir_all(&triggers).unwrap();
    std::fs::write(triggers.join("landing.cue"), LANDING_TRIGGER).unwrap();
    home
}

/// `.rk/checks.cue` and `.rk/repo.cue` are committed on `main` before any
/// branch forks off it, so both the rat's branch and the daemon's detached
/// gate worktree carry them. The named checks are the gates the landing queue
/// resolves BY NAME; they pass trivially because this test is about the
/// delivery seam, not about check content.
fn candidate_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# resumed generation\n").unwrap();
    git(dir, &["add", "README.md"]);
    git(dir, &["commit", "-m", "init"]);

    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        "checks: [\n    {name: \"landing-protected-paths\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"landing-diff-scope\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"verify\", command: \"true\", timeout: \"30s\"},\n]\n",
    )
    .unwrap();
    std::fs::write(rk_dir.join("repo.cue"), "repo: {}\n").unwrap();
    git(dir, &["add", ".rk/checks.cue", ".rk/repo.cue"]);
    git(
        dir,
        &[
            "commit",
            "-m",
            "test: register repository policy and landing checks",
        ],
    );
}

#[test]
fn resumed_generation_successor_lands_natively_and_survives_a_real_daemon_restart() {
    let home = daemon_home();
    let control = tempfile::tempdir().unwrap();
    let repo_root = tempfile::tempdir().unwrap();
    let home_path = home.path().to_path_buf();
    let control_path = control.path().to_path_buf();
    let repo = repo_root.path().join("resumed-gen-repo");
    let repo_name = "resumed-gen-repo";
    candidate_repo(&repo);
    // Declared after `home` so it drops first — see `DaemonGuard`.
    let _guard = DaemonGuard {
        home: home_path.clone(),
    };
    let (h, c) = (home_path.as_path(), control_path.as_path());

    // ---- Daemon A: a real detached process, started by the first `rk` call.
    let daemon_a = daemon_pid(h, c);
    json_stdout(
        &rk(h, c)
            .args(["--json", "repo", "add", repo.to_str().unwrap()])
            .output()
            .unwrap(),
    );
    let task = json_stdout(
        &rk(h, c)
            .args([
                "--json",
                "ticket",
                "new",
                "resumed generation successor",
                "--repo",
                repo_name,
            ])
            .output()
            .unwrap(),
    )["identity"]
        .as_str()
        .unwrap()
        .to_string();

    let spawned = json_stdout(
        &rk(h, c)
            .args([
                "--json",
                "spawn",
                "--task",
                &task,
                "--repo",
                repo.to_str().unwrap(),
                "--harness",
                "fake",
            ])
            .output()
            .unwrap(),
    );
    let rat = spawned["name"].as_str().unwrap().to_string();
    let spawn_id = spawned["spawn"].as_str().unwrap().to_string();

    let completed = until("the rat's first completion", || {
        let record = agent_record(h, c, &rat);
        assert_ne!(
            record["state"], "failed",
            "the rat failed instead of completing: {record}"
        );
        (record["state"] == "completed").then_some(record)
    });
    let branch = completed["branch"].as_str().unwrap().to_string();
    let target = completed["target_branch"].as_str().unwrap().to_string();
    let target_at_start = git(&repo, &["rev-parse", &target]);
    assert!(
        completed["merge_commit"].is_null(),
        "nothing may have landed yet — the resume below is the ticket's corrected ordering"
    );
    let head_1 = git(&repo, &["rev-parse", &branch]);

    // The first delivery is genuinely HELD PENDING inside native landing: its
    // diff is not doc-only, so the pipeline dispatched a native reviewer, and
    // that reviewer is blocked on its own head's release file.
    // `awaiting_review` is the durable proof of the hold.
    let held = until("the first candidate to reach awaiting_review", || {
        queue_rows(h, c, repo_name)
            .into_iter()
            .find(|row| row["branch"] == branch.as_str() && row["status"] == "awaiting_review")
    });
    assert_eq!(held["head_sha"].as_str(), Some(head_1.as_str()));
    assert_ne!(
        held["diff_class"], "doc-only",
        "a doc-only candidate would bypass review entirely — this one must not"
    );
    assert_eq!(
        git(&repo, &["rev-parse", &target]),
        target_at_start,
        "the first delivery must still be unlanded when the generation resumes"
    );

    // ---- The SAME generation resumes, BEFORE its first delivery lands.
    let respawned = json_stdout(&rk(h, c).args(["--json", "respawn", &rat]).output().unwrap());
    assert_eq!(
        respawned["spawn"].as_str(),
        Some(spawn_id.as_str()),
        "`rk respawn` must continue the same generation, not mint a new one"
    );
    let successor_row = until("the resumed generation's own landing candidate", || {
        queue_rows(h, c, repo_name).into_iter().find(|row| {
            row["branch"] == branch.as_str() && row["head_sha"].as_str() != Some(head_1.as_str())
        })
    });
    let head_2 = successor_row["head_sha"].as_str().unwrap().to_string();
    assert_eq!(
        successor_row["source_spawn"].as_str(),
        Some(spawn_id.as_str()),
        "the successor candidate must be bound to the SAME generation: {successor_row}"
    );
    assert_eq!(
        git(&repo, &["rev-parse", &branch]),
        head_2,
        "the successor source is a real commit the resumed rat made on its own branch"
    );
    assert!(
        is_ancestor(&repo, &head_1, &head_2),
        "the successor source must descend from the first source"
    );

    // ---- Release ONLY the first delivery's reviewer. It lands and finalizes.
    std::fs::write(control.path().join(format!("release-{head_1}")), "").unwrap();
    let merge_1 = until("the first delivery to finalize", || {
        agent_record(h, c, &rat)["merge_commit"]
            .as_str()
            .map(str::to_string)
    });
    assert_eq!(git(&repo, &["rev-parse", &target]), merge_1);
    until("the first delivery's ticket record", || {
        (ticket_delivery(h, c, &task).as_deref() == Some(merge_1.as_str())).then_some(())
    });

    // ---- Arm the post-advance interruption, THEN release the successor's
    // reviewer. Ordering, not timing: the successor's reviewer cannot return
    // a verdict — so the pipeline cannot reach the barrier — until the file
    // written on the next line exists, and the first delivery already went
    // through the same code path unarmed.
    std::fs::write(
        home.path().join("fault-barrier"),
        BARRIER_POST_TARGET_ADVANCE,
    )
    .unwrap();
    std::fs::write(control.path().join(format!("release-{head_2}")), "").unwrap();

    let reached = home.path().join("fault-barrier.reached");
    until(
        "daemon A to park at the post-target-advance barrier",
        || reached.exists().then_some(()),
    );
    let merge_2 = git(&repo, &["rev-parse", &target]);
    assert_ne!(
        merge_2, merge_1,
        "native review/gate must have advanced the target to the successor"
    );
    assert!(is_ancestor(&repo, &merge_1, &merge_2));
    assert_eq!(
        agent_record(h, c, &rat)["merge_commit"].as_str(),
        Some(merge_1.as_str()),
        "the barrier parks post-advance but pre-finalization: nothing may have settled yet"
    );
    assert_eq!(
        ticket_delivery(h, c, &task).as_deref(),
        Some(merge_1.as_str()),
        "the ticket's delivery evidence must not be overwritten ahead of the agent pointer — \
         that partial-write ordering is what the incident exposed"
    );
    let cost_before_crash = agent_record(h, c, &rat)["cost_usd"].as_f64().unwrap();

    // ---- Crash daemon A for real, parked in that exact window.
    assert!(process_alive(daemon_a));
    let _ = Command::new("kill")
        .args(["-9", &daemon_a.to_string()])
        .status();
    until("daemon A to actually die", || {
        (!process_alive(daemon_a)).then_some(())
    });
    {
        // The receipt survived the crash on disk, durably `landing` with its
        // already-landed candidate — the exact shape the production incident
        // left behind, read straight out of the on-disk store with NO daemon
        // running at all.
        let layout = rk_core::paths::Layout::at(&home_path);
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let rows = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity("landing_queue_entry"),
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "exactly the unfinalized successor must survive the crash: {rows:?}"
        );
        assert_eq!(rows[0].payload["status"], "landing");
        assert_eq!(
            rows[0].payload["candidate_sha"].as_str(),
            Some(merge_2.as_str())
        );
    }
    // Disarm: the REPLACEMENT daemon is the one that gets to finalize. Nothing
    // else is cleaned up — daemon B has to reclaim the crashed daemon's stale
    // pid file and socket itself, exactly as in the field.
    std::fs::remove_file(home.path().join("fault-barrier")).unwrap();
    std::fs::remove_file(&reached).unwrap();

    // ---- Daemon B: a genuinely different OS process over the same home.
    let daemon_b = daemon_pid(h, c);
    assert_ne!(
        daemon_a, daemon_b,
        "the replacement must be a new daemon process, not a reconnect"
    );

    // It recovers from the durable receipt and settles the resumed
    // generation's successor — the whole point of the fix. Before it, this
    // conflicted on every replay, forever.
    until("the restarted daemon to settle the successor", || {
        (agent_record(h, c, &rat)["merge_commit"].as_str() == Some(merge_2.as_str())).then_some(())
    });
    until("the ticket's delivery record to advance too", || {
        (ticket_delivery(h, c, &task).as_deref() == Some(merge_2.as_str())).then_some(())
    });
    until(
        "the recovered receipt to retire rather than busy-retry",
        || queue_rows(h, c, repo_name).is_empty().then_some(()),
    );
    let advances = scan(h, c, repo_name, "delivery_merge_pointer_advanced");
    assert_eq!(
        advances.len(),
        1,
        "exactly one successor advance: {advances:?}"
    );
    assert_eq!(advances[0]["payload"]["agent"].as_str(), Some(rat.as_str()));
    assert_eq!(
        advances[0]["payload"]["from_merge_commit"].as_str(),
        Some(merge_1.as_str())
    );
    assert_eq!(
        advances[0]["payload"]["to_merge_commit"].as_str(),
        Some(merge_2.as_str())
    );

    // ---- Daemon C: a third real process. The replay must change nothing.
    let target_after = git(&repo, &["rev-parse", &target]);
    assert_eq!(target_after, merge_2);
    let history = git(&repo, &["rev-list", "--count", &target]);
    let cost_after = agent_record(h, c, &rat)["cost_usd"].as_f64().unwrap();
    assert!(
        cost_after >= cost_before_crash,
        "recorded spending is monotonic: {cost_before_crash} -> {cost_after}"
    );

    let _ = rk(h, c).args(["daemon", "stop"]).output();
    until("daemon B to exit", || {
        (!process_alive(daemon_b)).then_some(())
    });
    let daemon_c = daemon_pid(h, c);
    assert_ne!(daemon_b, daemon_c, "daemon C must be a third real process");

    // An absence has to be observed over a window, not at one instant: hold
    // every invariant continuously while daemon C has a live reactor, landing
    // consumer and supervisor sweep running against this home.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        assert_eq!(
            git(&repo, &["rev-parse", &target]),
            target_after,
            "a replayed restart must not advance the target again"
        );
        assert_eq!(
            git(&repo, &["rev-list", "--count", &target]),
            history,
            "a replayed restart must not add a second merge commit"
        );
        let replayed = agent_record(h, c, &rat);
        assert_eq!(
            replayed["merge_commit"].as_str(),
            Some(merge_2.as_str()),
            "a replayed restart must not move the merge pointer again"
        );
        assert_eq!(
            replayed["cost_usd"].as_f64(),
            Some(cost_after),
            "a replayed restart must not re-charge the generation"
        );
        assert_eq!(
            ticket_delivery(h, c, &task).as_deref(),
            Some(merge_2.as_str()),
            "a replayed restart must not roll the ticket's delivery record back"
        );
        assert_eq!(
            scan(h, c, repo_name, "delivery_merge_pointer_advanced").len(),
            1,
            "a replayed restart must not record a second advance"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}
