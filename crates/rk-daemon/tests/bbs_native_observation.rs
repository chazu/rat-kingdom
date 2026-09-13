//! Executing acceptance for the S2 NATIVE producers and the three supervised
//! exposure surfaces, driven through a real daemon and real fake-harness
//! children rather than in-process against `Supervisor`.
//!
//! The unit tests in `supervisor.rs` prove the decision logic; they cannot
//! prove that a real `agent.spawn`/`agent.respawn`/`agent.continue_recovery`
//! actually reaches the fenced handlers, that a launch joins its own
//! `agent_final_usage`/`agent_exit` records, or that the four launch/resume
//! paths emit the session token a report has to join on. That is what this
//! file does.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// `RK_FAKE_HARNESS_CMD` is process-global, so the tests in this file take
/// turns rather than clobbering each other's fixture script mid-run.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn git(dir: &Path, args: &[&str]) {
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
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

async fn start(layout: &Layout) -> Client {
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    tokio::spawn(daemon.run());
    connect(layout).await
}

/// Every daemon-authored BBS record of one `bbs_kind`, in the order the store
/// returns them.
async fn observations(client: &mut Client, repo: &str, kind: &str) -> Vec<Value> {
    let scanned = client
        .call("space.scan", json!({"category": "event", "scope": repo}))
        .await
        .unwrap();
    scanned["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["bbs_kind"] == kind)
        .map(|t| t["payload"].clone())
        .collect()
}

/// Poll until `kind` has at least `want` records, so a test never races the
/// four independent hops between a child's last line and a durable record.
async fn wait_for(client: &mut Client, repo: &str, kind: &str, want: usize) -> Vec<Value> {
    for _ in 0..300 {
        let found = observations(client, repo, kind).await;
        if found.len() >= want {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "timed out waiting for {want} `{kind}` record(s); saw {:?}",
        observations(client, repo, kind).await
    );
}

/// Lifecycle events by tuple IDENTITY — `agent_spawned`/`agent_respawned`
/// carry no `bbs_kind`, so they are not reachable through [`observations`].
async fn lifecycle(client: &mut Client, repo: &str, identity: &str) -> Vec<Value> {
    let scanned = client
        .call("space.scan", json!({"category": "event", "scope": repo}))
        .await
        .unwrap();
    scanned["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["identity"] == identity)
        .map(|t| t["payload"].clone())
        .collect()
}

async fn status(client: &mut Client, name: &str) -> Value {
    client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap()
}

/// Two launches of ONE generation from one script, selected by an invocation
/// counter, because `RK_FAKE_HARNESS_CMD` is resolved once and reused verbatim
/// by the respawn.
///
/// Launch 1 declares `rk done` and only THEN reports its result: the
/// done-before-final-result case, where the disposition is already settled and
/// the provider's authoritative total arrives afterwards. Launch 2 reports its
/// own, different total. Neither may borrow the other's.
fn two_launch_fixture(marker: &Path) -> String {
    fixture::with_rk_done(&format!(
        r#"
read -r _prompt
printf 'launch\n' >> '{marker}'
count=$(wc -l < '{marker}' | tr -d ' ')
echo '{{"type":"system","subtype":"init","session_id":"provider-'"$count"'"}}'
if [ "$count" = "1" ]; then
  echo 'first' > gnawed.txt
  git add gnawed.txt >/dev/null 2>&1
  git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work"
  rk_done "declared before the provider's final total"
  sleep 1
  echo '{{"type":"result","subtype":"success","is_error":false,"result":"one","session_id":"provider-1","total_cost_usd":1.25,"usage":{{"input_tokens":50,"output_tokens":25,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
else
  rk_done "second launch"
  echo '{{"type":"result","subtype":"success","is_error":false,"result":"two","session_id":"provider-2","total_cost_usd":4.5,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
fi
"#,
        marker = marker.display(),
    ))
}

/// A launch that reports a result and then keeps spending before dying without
/// another one. The last total it reported is therefore a PARTIAL amount for
/// the launch, not its final cost.
const RESULT_THEN_MORE_WORK: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"provider-partial"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"paused mid-task","session_id":"provider-partial","total_cost_usd":0.75,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
echo '{"type":"assistant","message":{"role":"assistant","content":[],"usage":{"input_tokens":9000,"output_tokens":900,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
sleep 0.3
exit 9
"#;

/// A rat that commits work and only then loses its transport — the one shape
/// `detect_post_commit_outage` parks a recovery for.
const POST_COMMIT_OUTAGE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"pre-outage"}'
git config user.email rat@example.com
git config user.name Rat
echo 'work' > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
echo 'fatal: connection refused while contacting api' >&2
exit 1
"#;

/// A spawn and a respawn of the SAME generation, end to end: two physical
/// launches, two distinct session tokens, and each one joined to its own
/// final-usage and exit records — plus the `spawn`/`resume` exposure surfaces
/// both carrying the token a report joins on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_respawn_is_a_second_launch_of_one_generation_with_its_own_cost_and_exit() {
    let _guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    let marker = home.path().join("launches");
    std::env::set_var("RK_FAKE_HARNESS_CMD", two_launch_fixture(&marker));

    let layout = Layout::at(home.path());
    let mut client = start(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo_dir.path(), "task": "TKT-obs-1", "harness": "fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // --- Surface 1 of 4: `spawn`. ---
    let exposures = wait_for(&mut client, &repo, "exposure", 1).await;
    assert_eq!(exposures[0]["surface"], "spawn");
    assert_eq!(exposures[0]["bound"], "agent");
    assert_eq!(exposures[0]["agent"], name.as_str());
    assert_eq!(exposures[0]["task"], "TKT-obs-1");
    // Nothing relevant existed yet, and that is recorded as an EMPTY selection
    // — the whole point of `prepared: 0` being distinguishable from "no record
    // was ever written".
    assert_eq!(exposures[0]["prepared"], 0);
    assert_eq!(exposures[0]["entries"], json!([]));
    assert_eq!(exposures[0]["semantics"], "prepared");

    let first_usage = wait_for(&mut client, &repo, "agent_final_usage", 1).await;
    let first_exit = wait_for(&mut client, &repo, "agent_exit", 1).await;
    let session_one = first_usage[0]["session"].as_str().unwrap().to_string();
    let spawn_id = first_usage[0]["spawn"].as_str().unwrap().to_string();
    assert!(!session_one.is_empty() && !spawn_id.is_empty());

    // The done-before-final-result case: `rk done` settled the disposition a
    // second before the provider's authoritative total arrived, and the total
    // is observed anyway rather than lost to the provisional figure.
    assert_eq!(first_usage[0]["cost_usd"], 1.25);
    assert_eq!(
        first_usage[0]["cost_basis"], "provider_reported_segment_total",
        "the result carried a provider total, so that is the basis"
    );
    assert_eq!(first_usage[0]["semantics"], "reported_estimate_not_billed");
    assert_eq!(first_usage[0]["declared_done"], true);
    assert_eq!(first_usage[0]["stale_session"], false);
    assert_eq!(first_usage[0]["provider_session"], "provider-1");

    // Completion is NOT physical exit, and the exit record is the only place
    // that says the process is actually gone.
    assert_eq!(first_exit[0]["session"], session_one.as_str());
    assert_eq!(first_exit[0]["spawn"], spawn_id.as_str());
    assert_eq!(
        first_exit[0]["cost_coverage"], "final",
        "a result with no further model work after it covers the launch"
    );
    assert_eq!(first_exit[0]["stale_session"], false);
    assert_eq!(
        first_exit[0]["duration_semantics"],
        "process_lifetime_not_active_work"
    );
    assert!(
        first_exit[0]["launched_at"].as_str().is_some(),
        "the exit must carry THIS launch's own start time: {:?}",
        first_exit[0]
    );

    // The launch event a report joins FROM must name the same token, or the
    // observations below cannot be connected to a launch at all.
    let launches = lifecycle(&mut client, &repo, "agent_spawned").await;
    assert_eq!(launches.len(), 1, "{launches:?}");
    assert_eq!(
        launches[0]["session"],
        session_one.as_str(),
        "agent_spawned must name the launch its observations belong to"
    );
    assert_eq!(launches[0]["spawn"], spawn_id.as_str());
    assert!(launches[0]["launched_at"].as_str().is_some());

    // --- Surface 2 of 4: `resume`. A respawn CONTINUES the generation. ---
    for _ in 0..200 {
        if !status(&mut client, &name).await["agent"]["state"]
            .as_str()
            .is_some_and(|s| s == "running")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client
        .call("agent.respawn", json!({"name": name}))
        .await
        .unwrap();

    let exposures = wait_for(&mut client, &repo, "exposure", 2).await;
    let resume = exposures
        .iter()
        .find(|e| e["surface"] == "resume")
        .expect("a respawn must record a `resume` exposure");
    assert_eq!(resume["agent"], name.as_str());
    assert_eq!(
        resume["spawn"],
        spawn_id.as_str(),
        "a respawn keeps the generation, which is exactly why `spawn` alone \
         cannot identify a launch"
    );

    let usage = wait_for(&mut client, &repo, "agent_final_usage", 2).await;
    let session_two = usage
        .iter()
        .find(|u| u["session"] != session_one.as_str())
        .map(|u| u["session"].as_str().unwrap().to_string())
        .expect("the second launch must mint its own session token");
    assert_eq!(
        usage
            .iter()
            .filter(|u| u["spawn"] == spawn_id.as_str())
            .count(),
        2,
        "both launches belong to one generation: {usage:?}"
    );
    // Each launch reports its OWN segment total. Pooling them would double the
    // cost of one generation.
    let second = usage
        .iter()
        .find(|u| u["session"] == session_two.as_str())
        .unwrap();
    assert_eq!(second["cost_usd"], 4.5);
    assert_eq!(second["provider_session"], "provider-2");

    let exits = wait_for(&mut client, &repo, "agent_exit", 2).await;
    let mut exit_sessions: Vec<&str> = exits
        .iter()
        .map(|e| e["session"].as_str().unwrap())
        .collect();
    exit_sessions.sort_unstable();
    let mut want = vec![session_one.as_str(), session_two.as_str()];
    want.sort_unstable();
    assert_eq!(
        exit_sessions, want,
        "each physical launch connects its own exit, never the other's"
    );

    // And the resume path names its own token too — a same-`SpawnId` second
    // launch is only joinable because this differs from the first.
    let relaunches = lifecycle(&mut client, &repo, "agent_respawned").await;
    assert_eq!(relaunches.len(), 1, "{relaunches:?}");
    assert_eq!(relaunches[0]["session"], session_two.as_str());
    assert_eq!(
        relaunches[0]["spawn"],
        spawn_id.as_str(),
        "the generation is continued, not replaced"
    );
    assert_ne!(session_one, session_two);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// The edge case the ticket singles out: a reported result, then more model
/// usage, then a death with no later result. The known total is a PARTIAL
/// amount and the remainder must stay unknown — finality is never inferred
/// from merely finding some result before an exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_that_more_work_ran_past_is_partial_not_final() {
    let _guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", RESULT_THEN_MORE_WORK);

    let layout = Layout::at(home.path());
    let mut client = start(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "agent.spawn",
            json!({"repo": repo_dir.path(), "task": "TKT-obs-2", "harness": "fake"}),
        )
        .await
        .unwrap();

    let usage = wait_for(&mut client, &repo, "agent_final_usage", 1).await;
    // The turn reported without a `rk done`, so it is a PAUSE, not a finish —
    // and a paused result is exactly the one the partial case hinges on.
    assert_eq!(usage[0]["state"], "paused");
    assert_eq!(usage[0]["declared_done"], false);
    assert_eq!(usage[0]["cost_usd"], 0.75);

    let exits = wait_for(&mut client, &repo, "agent_exit", 1).await;
    assert_eq!(
        exits[0]["cost_coverage"], "partial_unknown",
        "9k more input tokens ran past the reported total: {:?}",
        exits[0]
    );
    assert_eq!(exits[0]["session"], usage[0]["session"]);
    assert_eq!(
        exits[0]["exit_code"], 9,
        "a nonzero code is not a signal and must not be reported as null"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// --- Surface 3 of 4: `recovery`. A continuation after a transport outage is
/// its own launch, and it records its own exposure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_continued_recovery_records_the_recovery_exposure_surface() {
    let _guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", POST_COMMIT_OUTAGE);

    let layout = Layout::at(home.path());
    let mut client = start(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo_dir.path(), "task": "TKT-obs-3", "harness": "fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    for _ in 0..200 {
        if !status(&mut client, &name).await["agent"]["recovery"].is_null() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !status(&mut client, &name).await["agent"]["recovery"].is_null(),
        "a post-commit transport outage must park a durable recovery record"
    );

    // The launch that died reported no result at all: "we know there was none"
    // is a different finding from "we cannot say".
    let exits = wait_for(&mut client, &repo, "agent_exit", 1).await;
    assert_eq!(exits[0]["cost_coverage"], "none");
    assert!(
        observations(&mut client, &repo, "agent_final_usage")
            .await
            .is_empty(),
        "no result was reported, so no final-usage record may be invented"
    );

    client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "op-1", "harness": Value::Null}),
        )
        .await
        .unwrap();

    let exposures = wait_for(&mut client, &repo, "exposure", 2).await;
    let recovery = exposures
        .iter()
        .find(|e| e["surface"] == "recovery")
        .unwrap_or_else(|| {
            panic!("a continued recovery must record its own exposure: {exposures:?}")
        });
    assert_eq!(recovery["agent"], name.as_str());
    assert_eq!(recovery["task"], "TKT-obs-3");
    assert_eq!(recovery["bound"], "agent");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
