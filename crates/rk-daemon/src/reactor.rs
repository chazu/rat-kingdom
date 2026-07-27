//! The daemon tuple-reactor: registered `#Trigger` reactions that fire
//! workflows when matching tuples land in the space. Zero-token, zero-model
//! dispatch — the keystone the stigmergy proposals (quorum promotion, obstacle
//! coalescence, convention injection) all ride on.
//!
//! # Why dispatch is scan-driven, not feed-driven
//!
//! The live feed ([`Space::subscribe`]) is a lossy broadcast: it drops events
//! for laggy consumers. A trigger must never miss an event, so the feed is used
//! only as a *wake signal*. The source of truth is a durable cursor over the
//! store: each cycle scans tuples with `id` greater than the last processed id
//! (ULIDs sort by creation time), fires matching triggers, then advances the
//! cursor. This is the same cursor discipline the multiplayer sync loop uses.
//!
//! # Idempotency and re-entrancy
//!
//! Dispatch is at-least-once (a crash mid-cycle re-runs from the saved cursor),
//! made idempotent by a durable marker written per fired `(trigger, tuple)`:
//! `already_fired` short-circuits a repeat. Re-entrancy — a workflow whose
//! output re-fires its own trigger — is broken three ways: the reactor tags its
//! own output with the reserved `reactor` instance (never reacted to), triggers
//! and config can exclude specific authors, and every trigger has a per-window
//! fire cap (`maxFires`, <=100) mirroring the `repeat` discipline.

use crate::agents::AgentRecord;
use crate::repos::RepoRegistry;
use crate::supervisor::Supervisor;
use crate::tickets::{NewTicket, Tickets};
use crate::workflow_exec::WorkflowEngine;
use rk_core::config::ReactorConfig;
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple, SYSTEM_SCOPE};
use rk_space::Space;
use rk_workflow::Trigger;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, info, warn};

/// The reserved author of every tuple the reactor writes (markers, obstacles).
/// Triggers never react to it, so a reaction can never fire on its own output.
pub const REACTOR_INSTANCE: &str = "reactor";
/// Identity of the durable idempotency marker tuples (system scope).
const MARKER_IDENTITY: &str = "reactor_fired";
/// Identity of a steward escalation `need` (steward.cue writes `rk out need
/// <repo> steward`): the discriminator for the built-in desktop-push reaction.
/// A rat's own `rk need` carries identity = its agent name, so this never
/// collides with an ordinary help request.
const STEWARD_ESCALATION_IDENTITY: &str = "steward";
/// Identity of the durable "this topic was already coalesced into a ticket"
/// marker (system scope). Bridges the window between filing and the ticket
/// landing so a feed-woken re-scan cannot file the same topic twice.
const COALESCE_FILED_IDENTITY: &str = "reactor_coalesced";
/// How long the "already filed" marker lives. Only needs to outlast the async
/// ticket-create + a cycle or two; the still-open ticket is the real
/// files-once-until-closed guard beyond that.
const COALESCE_FILED_TTL_SECS: i64 = 10 * 60;

/// A loaded trigger plus where it came from (a repo-local file defaults its
/// target repo to that repo; a global-dir trigger has no default repo).
#[derive(Clone)]
struct Loaded {
    trigger: Trigger,
    source_repo: Option<String>,
}

/// One candidate trigger file and the repo it belongs to (`None` = global dir).
type TriggerFile = (PathBuf, Option<String>);

/// A change-detection stamp for one trigger file: its path, owning repo, and
/// `(mtime, len)`. Reloading the parsed triggers (a `cue` shell-out per file)
/// is the reactor's dominant per-wake cost, so we reparse only when this stamp
/// changes. `len` rides alongside `mtime` to catch a same-second edit that
/// mtime's coarse (often 1s) granularity would otherwise miss.
type FileStamp = (PathBuf, Option<String>, Option<SystemTime>, Option<u64>);

/// Parsed triggers plus the file stamps they were parsed from. A cycle reuses
/// `triggers` whenever the freshly-computed stamps equal `stamps`.
struct TriggerCache {
    stamps: Vec<FileStamp>,
    triggers: Vec<Loaded>,
}

pub struct Reactor {
    space: Space,
    engine: Arc<WorkflowEngine>,
    tickets: Arc<Tickets>,
    /// The live-session owner, used to steer running rats when a convention is
    /// promoted at quorum (TKT-34). `None` in unit tests that never promote.
    supervisor: Option<Arc<Supervisor>>,
    layout: Layout,
    config: ReactorConfig,
    cursor_file: PathBuf,
    /// Per-trigger fire timestamps for the rolling rate cap. In-memory: a storm
    /// is a live-daemon phenomenon, and a restart legitimately resets the window.
    fires: Mutex<HashMap<String, Vec<Instant>>>,
    /// Parsed triggers cached across cycles, reparsed only when a trigger file's
    /// stamp changes (see [`TriggerCache`]). Skips the `cue` shell-outs on every
    /// steady-state wake.
    trigger_cache: Mutex<Option<TriggerCache>>,
    /// The relevant-category populations observed at the end of the previous
    /// cycle: `(promote_pop, coalesce_pop)`. `None` before the first cycle. The
    /// whole-store recomputes (quorum promotion, obstacle coalescence) run only
    /// when their population changed since last cycle (or on the first cycle, to
    /// catch up on any backlog that reached quorum while the reactor was down),
    /// so a burst of unrelated writes no longer forces a full-store rescan.
    last_pops: Mutex<Option<(u64, u64)>>,
}

impl Reactor {
    pub fn new(
        space: Space,
        engine: Arc<WorkflowEngine>,
        tickets: Arc<Tickets>,
        supervisor: Option<Arc<Supervisor>>,
        layout: Layout,
        config: ReactorConfig,
    ) -> Self {
        let cursor_file = layout.home().join("reactor-cursor");
        Self {
            space,
            engine,
            tickets,
            supervisor,
            layout,
            config,
            cursor_file,
            fires: Mutex::new(HashMap::new()),
            trigger_cache: Mutex::new(None),
            last_pops: Mutex::new(None),
        }
    }

    /// Baseline the cursor to the newest existing tuple so a fresh daemon does
    /// not react to the entire pre-existing backlog on first boot. A no-op once
    /// a cursor file exists (restarts resume where they left off).
    pub fn initialize_cursor(&self) -> rk_core::Result<()> {
        if self.cursor_file.exists() {
            return Ok(());
        }
        let all = self.space.scan(&Pattern::default())?;
        if let Some(max) = all.iter().map(|t| t.id).max() {
            self.save_cursor(max)?;
        }
        Ok(())
    }

    /// Process every tuple newer than the cursor: match it against all loaded
    /// triggers and fire the workflows. Returns how many workflows were fired.
    pub fn run_cycle(&self) -> rk_core::Result<usize> {
        let cursor = self.load_cursor();
        // Bounded delta: only tuples newer than the cursor, resolved from the id
        // PRIMARY KEY index — no full-table scan + Rust-side `id <= cursor` skip.
        let delta = self.space.scan(&Pattern::default().after(cursor))?;
        // Load the registry, then the triggers (cache-gated, so a `cue` shell-out
        // runs only when a trigger file changed), AFTER the delta scan. Loading
        // them no earlier than the scan closes the window where a repo / trigger
        // registered just before a tuple landed would be missed and the tuple
        // dropped as the cursor advances past it.
        let registry = RepoRegistry::load(&self.layout.home().join("repos.json"))?;
        let triggers = self.cached_triggers(&registry);

        let mut fired = 0usize;
        let mut max_id = cursor;
        let mut retryable_failure = false;
        for tuple in &delta {
            // Advance the cursor past every delta tuple, including the reactor's
            // own markers, so they are seen once and never re-scanned.
            max_id = Some(match max_id {
                Some(m) => m.max(tuple.id),
                None => tuple.id,
            });
            if tuple.instance == REACTOR_INSTANCE {
                continue;
            }
            // Built-in active push: a steward escalation gets a desktop
            // notification on top of its `rk inbox` row. Runs before the
            // configured triggers (it is orthogonal to them) and never aborts
            // the cycle — a herdr hiccup must not stall dispatch.
            if let Err(e) = self.notify_escalation(tuple) {
                warn!(tuple = %tuple.id, error = %e, "reactor escalation notify failed");
            }
            for loaded in &triggers {
                match self.try_fire(loaded, tuple, &registry) {
                    Ok(true) => fired += 1,
                    Ok(false) => {}
                    Err(e) => {
                        retryable_failure = true;
                        warn!(trigger = %loaded.trigger.name, error = %e, "reactor dispatch failed")
                    }
                }
            }
            // Built-in resolution-backlink reaction (TKT-28): an artifact that
            // resolves a wall retires it and lays a decaying topic->artifact
            // trail; a fresh obstacle/need on a topic that already has a trail
            // steers the reporting rat to the prior fix. Both are idempotent, so
            // a crash-replay from the saved cursor cannot double-apply them.
            let outcome = match tuple.category {
                Category::Artifact => self.link_resolution(tuple),
                Category::Obstacle | Category::Need => self.steer_from_resolution(tuple),
                _ => Ok(false),
            };
            if let Err(e) = outcome {
                retryable_failure = true;
                warn!(tuple = %tuple.id, error = %e, "reactor resolution-backlink failed");
            }
        }
        if !retryable_failure {
            if let Some(m) = max_id {
                if cursor.map(|c| m > c).unwrap_or(true) {
                    self.save_cursor(m)?;
                }
            }
        }

        // Whole-store recomputes (quorum promotion, obstacle coalescence). Their
        // INPUT is deliberately the whole store, not the cursor delta: a
        // suggestion / wall that reached quorum while the reactor was down still
        // promotes / files, and the promote-once guard is the durable Convention
        // / open ticket, not the cursor. But re-scanning + materialising the whole
        // store on EVERY wake is the cost TKT-29 targets. Gate WHETHER to
        // recompute on whether the relevant category population *changed* since
        // last cycle — an exact SQL COUNT (no row materialisation, and immune to
        // the same-millisecond ULID ordering that makes a cursor delta an
        // unreliable change signal, unlike the firing loop which tolerates it).
        // A promotion / coalescence can only newly qualify when an endorsement /
        // obstacle is ADDED, which moves the count; a burst of unrelated writes
        // leaves it unchanged, so the full scan is skipped. The first cycle
        // (`None`) always recomputes, catching up any pre-existing backlog.
        let promote_pop = self
            .space
            .count_in_categories(&[Category::Endorsement, Category::Suggestion])?;
        let coalesce_pop = self
            .space
            .count_in_categories(&[Category::Obstacle, Category::Need])?;
        let (changed_promote, changed_coalesce) = {
            let mut last = self.last_pops.lock().unwrap_or_else(|p| p.into_inner());
            let changed = match *last {
                None => (true, true),
                Some((lp, lc)) => (promote_pop != lp, coalesce_pop != lc),
            };
            *last = Some((promote_pop, coalesce_pop));
            changed
        };
        let need_promote = self.config.quorum > 0 && changed_promote;
        let need_coalesce = self.config.coalesce_quorum > 0 && changed_coalesce;
        if need_promote || need_coalesce {
            let all = self.space.scan(&Pattern::default())?;
            if need_promote {
                if let Err(e) = self.promote_conventions(&all) {
                    warn!(error = %e, "reactor quorum promotion failed");
                }
            }
            if need_coalesce {
                if let Err(e) = self.coalesce_obstacles(&all) {
                    warn!(error = %e, "reactor obstacle coalescence failed");
                }
            }
        }
        Ok(fired)
    }

    /// Promote any `Suggestion` that has reached quorum into a `Convention`.
    ///
    /// The count is DISTINCT endorser (`instance`) per suggestion (`identity`),
    /// recomputed from the passed snapshot — re-endorsing, or a duplicate
    /// endorsement tuple, can never inflate the tally. Idempotent: a suggestion
    /// that already has a `Convention` (matched by identity) is skipped, so the
    /// permanent Convention is itself the "already promoted" marker. Returns how
    /// many suggestions were promoted this call.
    fn promote_conventions(&self, all: &[Tuple]) -> rk_core::Result<usize> {
        if self.config.quorum == 0 {
            return Ok(0);
        }
        let quorum = self.config.quorum as usize;

        // suggestion id -> distinct endorser instances.
        let mut endorsers: HashMap<&str, HashSet<&str>> = HashMap::new();
        // suggestion ids that already have a Convention (idempotency guard).
        let mut promoted_ids: HashSet<&str> = HashSet::new();
        // suggestion id -> the Suggestion tuple, for citing its text.
        let mut suggestions: HashMap<&str, &Tuple> = HashMap::new();
        for t in all {
            if t.scope != SYSTEM_SCOPE {
                continue;
            }
            match t.category {
                Category::Endorsement => {
                    endorsers
                        .entry(t.identity.as_str())
                        .or_default()
                        .insert(t.instance.as_str());
                }
                Category::Convention => {
                    promoted_ids.insert(t.identity.as_str());
                }
                Category::Suggestion => {
                    suggestions.insert(t.identity.as_str(), t);
                }
                _ => {}
            }
        }

        let mut promoted = 0usize;
        // Newly promoted `(scope, text)` for the one steer sweep at the end: a rat
        // already RUNNING when a suggestion crosses quorum won't see the norm until
        // respawn (TKT-18 injects only at spawn), so we push the delta into its
        // live session now (TKT-34).
        let mut steer_deltas: Vec<(String, String)> = Vec::new();
        for (sug_id, instances) in &endorsers {
            if instances.len() < quorum || promoted_ids.contains(sug_id) {
                continue;
            }
            // Sorted, deduped endorser list for a stable, replay-safe citation.
            let endorser_list: BTreeSet<&str> = instances.iter().copied().collect();
            // The suggestion's own text may already have decayed; cite what we
            // still have (the endorsements alone carry the quorum).
            let text = suggestions
                .get(sug_id)
                .and_then(|s| s.payload.get("text").cloned())
                .unwrap_or(Value::Null);
            let convention = Tuple::new(
                Category::Convention,
                SYSTEM_SCOPE,
                *sug_id,
                REACTOR_INSTANCE,
                json!({
                    "suggestion": sug_id,
                    "text": text,
                    "endorsers": endorser_list.iter().collect::<Vec<_>>(),
                    "count": instances.len(),
                    "quorum": quorum,
                }),
            )
            // Furniture: a promoted norm is permanent, never `in`-consumable, and
            // replicates across castles via rk-sync for free.
            .with_lifecycle(Lifecycle::Furniture);
            let scope = convention.scope.clone();
            self.space.out(convention)?;
            promoted += 1;
            // Only steer on a materially-texted norm: a decayed (blank) suggestion
            // carries no guidance to inject, matching TKT-18's blank-text drop.
            if let Some(text) = text.as_str().map(str::trim).filter(|t| !t.is_empty()) {
                steer_deltas.push((scope, text.to_string()));
            }
            info!(
                suggestion = %sug_id,
                count = instances.len(),
                quorum,
                "reactor promoted suggestion to convention at quorum"
            );
        }

        // Steer live rats with the newly promoted norms. Best-effort and
        // fire-and-forget: the durable Convention is the once-per-norm guard (a
        // replay finds it in `promoted_ids` and never re-steers), so this runs at
        // most once per promotion. Skipped entirely when no supervisor is wired
        // (unit tests) or nothing materially new was promoted.
        if let Some(supervisor) = self.supervisor.clone() {
            if !steer_deltas.is_empty() {
                tokio::runtime::Handle::current().spawn(async move {
                    for (scope, text) in steer_deltas {
                        Self::steer_convention(&supervisor, &scope, &text).await;
                    }
                });
            }
        }
        Ok(promoted)
    }

    /// Push a newly promoted convention into every live rat in its scope via the
    /// supervisor's `steer` path — the same mid-session injection `rk steer`
    /// uses. A system-scope norm reaches every live rat; a repo-scope norm only
    /// rats in that repo. Best-effort: a rat with no live session (raced into
    /// completion, attach without target) is skipped, never fatal.
    async fn steer_convention(supervisor: &Supervisor, scope: &str, text: &str) {
        let message = convention_steer_message(text);
        let agents = supervisor.list();
        let targets = convention_steer_targets(&agents, scope);
        for name in &targets {
            match supervisor.steer(name, &message).await {
                Ok(()) => debug!(rat = %name, scope, "reactor steered rat with promoted convention"),
                Err(e) => {
                    debug!(rat = %name, error = %e, "reactor convention steer skipped (no live session)")
                }
            }
        }
        if !targets.is_empty() {
            info!(scope, rats = targets.len(), "reactor steered live rats with a promoted convention");
        }
    }

    /// Coalesce the flat obstacle/need pile: repeated reports of one wall become
    /// a single durable ticket instead of ten equal, signal-less obstacles.
    ///
    /// Every cycle this buckets all obstacle/need tuples by a normalised topic
    /// key (`scope` + normalised `payload.text`), counting DISTINCT reporters
    /// (`instance`) per topic — recomputed from the passed snapshot, so a
    /// re-stated obstacle from the same rat can never inflate the tally. When a
    /// topic reaches `coalesce_quorum` distinct reporters it files ONE ticket
    /// linking the contributing tuples. (The sub-quorum "how hot is this wall"
    /// gradient already lives in the raw obstacles' own decaying strength, which
    /// a strength-sorted scan ranks; coalescence only escalates a wall that many
    /// rats converge on into the durable backlog — it never injects synthetic
    /// obstacles that would pollute that pile.)
    ///
    /// Filing is idempotent two ways: a synchronous durable "already filed"
    /// marker written before the (async) create bridges the create latency, and
    /// an already-open ticket carrying the same `coalesce_key` suppresses
    /// re-filing until it is closed. Returns how many tickets were filed.
    fn coalesce_obstacles(&self, all: &[Tuple]) -> rk_core::Result<usize> {
        let quorum = self.config.coalesce_quorum as usize;
        if quorum == 0 {
            return Ok(0);
        }

        // (scope, topic) -> distinct reporter instances, plus the contributing
        // tuples for citation. A rat's obstacle is keyed on identity=agent, so it
        // holds at most one trail per topic; counting distinct instances is
        // "how many rats hit this wall".
        struct Bucket<'a> {
            reporters: BTreeSet<&'a str>,
            members: Vec<&'a Tuple>,
            sample: &'a str,
        }
        let mut buckets: HashMap<(String, String), Bucket> = HashMap::new();
        for t in all {
            if t.instance == REACTOR_INSTANCE {
                continue;
            }
            if !matches!(t.category, Category::Obstacle | Category::Need) {
                continue;
            }
            let Some(text) = t.payload.get("text").and_then(Value::as_str) else {
                continue;
            };
            let topic = normalize_topic(text);
            if topic.is_empty() {
                continue;
            }
            let bucket = buckets
                .entry((t.scope.clone(), topic))
                .or_insert_with(|| Bucket {
                    reporters: BTreeSet::new(),
                    members: Vec::new(),
                    sample: text,
                });
            bucket.reporters.insert(t.instance.as_str());
            bucket.members.push(t);
        }

        // Dedupe: a topic already filed (open ticket carrying its coalesce_key,
        // OR a still-live "already filed" marker) is not filed again.
        let open_keys: HashSet<&str> = all
            .iter()
            .filter(|t| t.category == Category::Task && !ticket_is_done(t))
            .filter_map(|t| t.payload.get("coalesce_key").and_then(Value::as_str))
            .collect();
        let filed_keys: HashSet<&str> = all
            .iter()
            .filter(|t| {
                t.category == Category::Event
                    && t.scope == SYSTEM_SCOPE
                    && t.identity == COALESCE_FILED_IDENTITY
            })
            .filter_map(|t| t.payload.get("key").and_then(Value::as_str))
            .collect();

        let mut filed = 0usize;
        for ((scope, topic), bucket) in &buckets {
            let count = bucket.reporters.len();
            if count < quorum {
                continue;
            }
            let key = coalesce_key(scope, topic);
            if open_keys.contains(key.as_str()) || filed_keys.contains(key.as_str()) {
                continue;
            }
            match self.file_coalesced_ticket(scope, topic, &key, &bucket.members, bucket.sample) {
                Ok(()) => {
                    filed += 1;
                    info!(topic = %topic, scope = %scope, count, quorum, "reactor coalesced obstacles into a ticket");
                }
                Err(e) => warn!(topic = %topic, error = %e, "reactor: coalesced ticket filing failed"),
            }
        }
        Ok(filed)
    }

    /// File exactly one coalesced ticket for a topic at quorum. Writes the
    /// synchronous "already filed" guard marker BEFORE spawning the (async)
    /// ticket create, so a feed-woken re-scan between now and the ticket landing
    /// still sees the topic as filed and skips it.
    fn file_coalesced_ticket(
        &self,
        scope: &str,
        topic: &str,
        key: &str,
        members: &[&Tuple],
        sample: &str,
    ) -> rk_core::Result<()> {
        let mut reporters: BTreeSet<&str> = BTreeSet::new();
        let mut tuple_ids: Vec<String> = Vec::new();
        for m in members {
            reporters.insert(m.instance.as_str());
            tuple_ids.push(m.id.to_string());
        }
        let body = format!(
            "Auto-filed by the reactor: {n} rat(s) hit the same wall.\n\n\
             Topic: {topic}\nScope: {scope}\nExample report: {sample}\n\n\
             Reporters: {reporters}\nContributing tuples: {tuples}",
            n = reporters.len(),
            reporters = reporters.iter().copied().collect::<Vec<_>>().join(", "),
            tuples = tuple_ids.join(", "),
        );
        // The coalesce_key rides in the ticket payload so a still-open ticket is
        // itself the files-once-until-closed guard on the next cycle.
        let new = NewTicket {
            title: format!("Coalesced obstacle: {}", truncate(sample, 80)),
            body: Some(body),
            scope: scope.to_string(),
            parent: None,
            priority: "normal".to_string(),
            labels: vec!["obstacle-coalesce".to_string()],
            depends_on: Vec::new(),
            created_by: Some(REACTOR_INSTANCE.to_string()),
            coalesce_key: Some(key.to_string()),
        };

        // Guard marker first — synchronous and durable, so idempotency does not
        // depend on the async create having completed.
        let mut guard = Tuple::new(
            Category::Event,
            SYSTEM_SCOPE,
            COALESCE_FILED_IDENTITY,
            REACTOR_INSTANCE,
            json!({"key": key, "topic": topic, "scope": scope}),
        )
        .with_lifecycle(Lifecycle::Ephemeral);
        guard.expires_at =
            Some(chrono::Utc::now() + chrono::Duration::seconds(COALESCE_FILED_TTL_SECS));
        self.space.out(guard)?;

        // Create runs through Tickets so ticket-id allocation stays serialized
        // with every other create. run_cycle executes with the runtime context
        // entered (server wraps it in `handle.enter()`), so a spawn is safe here;
        // a synchronous block would deadlock the create's async lock.
        let tickets = Arc::clone(&self.tickets);
        tokio::runtime::Handle::current().spawn(async move {
            if let Err(e) = tickets.create(new).await {
                warn!(error = %e, "reactor: coalesced ticket create failed");
            }
        });
        Ok(())
    }

    /// React to a resolving artifact (TKT-28, stigmergy P8): retire the exact
    /// obstacle/need it names in `payload.resolves` and lay a decaying
    /// `(topic -> this artifact)` trail so the next rat hitting the same wall is
    /// steered to the prior fix instead of redoing it.
    ///
    /// Idempotent: the wall delete is a no-op once gone, and the trail write is
    /// an upsert (reinforce) keyed on `(scope, topic)`, so re-resolving a topic
    /// refreshes the single trail rather than piling up duplicates — and a
    /// crash-replay of this artifact simply re-lays the same trail. Returns
    /// whether a trail was laid.
    fn link_resolution(&self, artifact: &Tuple) -> rk_core::Result<bool> {
        let Some(resolves) = artifact.payload.get("resolves").and_then(Value::as_str) else {
            return Ok(false);
        };
        let Ok(target) = resolves.parse::<RecordId>() else {
            warn!(artifact = %artifact.id, resolves = %resolves, "reactor: --resolves is not a valid tuple id");
            return Ok(false);
        };
        // Find the wall by id. Already retired / evaporated => nothing to key a
        // trail on; the backlink still lives in the artifact payload.
        let Some(wall) = self.find_wall(target)? else {
            return Ok(false);
        };
        let Some(text) = wall.payload.get("text").and_then(Value::as_str) else {
            return Ok(false);
        };
        let topic = normalize_topic(text);
        if topic.is_empty() {
            return Ok(false);
        }

        // Retire the solved wall, then lay/reinforce the resolution trail.
        self.space.delete(wall.id)?;
        let trail = Tuple::new(
            Category::Resolution,
            artifact.scope.clone(),
            topic.clone(),
            REACTOR_INSTANCE,
            json!({
                "topic": topic,
                "artifact": artifact.identity,
                "artifact_id": artifact.id.to_string(),
                "scope": artifact.scope,
                "resolved": wall.id.to_string(),
                "resolved_category": wall.category.as_str(),
                "text": text,
            }),
        )
        // Ephemeral + a decaying strength (reinforce sets FULL): a trail nobody
        // re-needs fades on its own via the GC decay (TKT-14), no expiry needed.
        .with_lifecycle(Lifecycle::Ephemeral);
        self.space.reinforce(trail)?;
        info!(artifact = %artifact.id, wall = %wall.id, topic = %topic, "reactor linked resolution and retired the wall");
        Ok(true)
    }

    /// React to a fresh obstacle/need (TKT-28): if its topic already has a
    /// resolution trail, reinforce that trail (a rat hit this wall again, so it
    /// is still live) and steer the reporting rat to the resolving artifact with
    /// a directed message. Returns whether a steer was emitted.
    fn steer_from_resolution(&self, wall: &Tuple) -> rk_core::Result<bool> {
        let Some(text) = wall.payload.get("text").and_then(Value::as_str) else {
            return Ok(false);
        };
        let topic = normalize_topic(text);
        if topic.is_empty() {
            return Ok(false);
        }
        // A prior resolution for this exact (scope, topic)?
        let trail_pat = Pattern::category(Category::Resolution)
            .scope(&wall.scope)
            .identity(&topic);
        let Some(trail) = self.space.scan(&trail_pat)?.into_iter().next() else {
            return Ok(false);
        };
        // Idempotency guard: one steer per (obstacle tuple, resolution). Keyed on
        // this wall's id, so a crash-replay of THIS tuple is suppressed while a
        // genuinely new obstacle from another rat still steers (and reinforces).
        let mut seen = Pattern::category(Category::Message)
            .scope(&wall.scope)
            .identity(&wall.instance);
        seen.payload_search = Some(format!("\"obstacle\":\"{}\"", wall.id));
        if !self.space.scan(&seen)?.is_empty() {
            return Ok(false);
        }

        let refreshed = self.space.reinforce(trail.clone())?;
        let artifact_id = trail
            .payload
            .get("artifact_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let artifact_name = trail
            .payload
            .get("artifact")
            .and_then(Value::as_str)
            .unwrap_or("?");
        let steer = Tuple::new(
            Category::Message,
            wall.scope.clone(),
            wall.instance.clone(), // directed at the reporting rat
            REACTOR_INSTANCE,
            json!({
                "type": "resolution_steer",
                "text": format!(
                    "This wall was resolved before — see artifact '{artifact_name}' ({artifact_id}) before redoing the work."
                ),
                "topic": topic,
                "artifact": artifact_name,
                "artifact_id": artifact_id,
                "resolution": refreshed.id.to_string(),
                "obstacle": wall.id.to_string(),
            }),
        );
        self.space.out(steer)?;
        info!(wall = %wall.id, topic = %topic, artifact = %artifact_id, "reactor steered rat to prior resolution");
        Ok(true)
    }

    /// Find an obstacle or need by exact tuple id. Resolving artifacts are rare,
    /// so a per-category scan filtered to the id is cheap enough and avoids a
    /// dedicated by-id index.
    fn find_wall(&self, id: RecordId) -> rk_core::Result<Option<Tuple>> {
        for cat in [Category::Obstacle, Category::Need] {
            if let Some(t) = self
                .space
                .scan(&Pattern::category(cat))?
                .into_iter()
                .find(|t| t.id == id)
            {
                return Ok(Some(t));
            }
        }
        Ok(None)
    }

    /// Decide and, if warranted, dispatch a single trigger against one tuple.
    /// Returns whether a workflow was fired.
    fn try_fire(
        &self,
        loaded: &Loaded,
        tuple: &Tuple,
        registry: &RepoRegistry,
    ) -> rk_core::Result<bool> {
        let trigger = &loaded.trigger;
        if self.excluded(trigger, tuple) {
            return Ok(false);
        }
        if !self.pattern(trigger)?.matches(tuple) {
            return Ok(false);
        }
        // Idempotency: one dispatch per (trigger, tuple) for the marker's life.
        let key = format!("{}@{}", trigger.name, tuple.id);
        if self.already_fired(&key)? {
            return Ok(false);
        }
        if self.rate_limited(trigger) {
            warn!(trigger = %trigger.name, "reactor rate cap reached; skipping fire");
            let _ = self.space.out(Tuple::new(
                Category::Obstacle,
                SYSTEM_SCOPE,
                "reactor_rate_capped",
                REACTOR_INSTANCE,
                json!({"trigger": trigger.name, "window_secs": self.config.window_secs}),
            ));
            return Err(rk_core::Error::other(format!(
                "reactor trigger '{}' is rate limited",
                trigger.name
            )));
        }
        // Target repo: explicit override > the trigger file's own repo > the
        // matched tuple's scope. It must resolve to a registered repo path.
        let repo_name = trigger
            .repo
            .clone()
            .or_else(|| loaded.source_repo.clone())
            .unwrap_or_else(|| tuple.scope.clone());
        let Some(record) = registry.get(&repo_name) else {
            return Err(rk_core::Error::other(format!(
                "reactor trigger '{}' targets unregistered repo '{}'",
                trigger.name, repo_name
            )));
        };
        let repo_path = record.path.to_string_lossy().to_string();
        let params = template_params(&trigger.params, tuple);

        // The workflow runs in the background; run() returns the instance now.
        let instance = self.engine.run(&trigger.run, &repo_path, params)?;
        info!(
            trigger = %trigger.name,
            workflow = %trigger.run,
            instance = %instance.id,
            tuple = %tuple.id,
            "reactor fired workflow"
        );
        self.mark_fired(&key, trigger, tuple, &instance.id)?;
        self.record_fire(&trigger.name);
        Ok(true)
    }

    /// Built-in reaction: push a desktop notification when the steward
    /// escalates to the operator. TKT-19 surfaces STOP/unknown verdicts as a
    /// `need` that `rk inbox` ranks — a passive queue. This adds the active
    /// push the leverage doc calls for, so a human is pinged the moment a
    /// branch needs a merge decision instead of only on the next inbox check.
    ///
    /// Fires at most once per need tuple, guarded by the same durable marker
    /// the trigger path uses (so an at-least-once re-scan never double-pops).
    /// A reinforced escalation keeps its id below the cursor, so it is never
    /// re-seen — repeat pushes only happen after the old need evaporates and a
    /// fresh one is written, which is the intended de-spam. Degrades to a
    /// no-op with no herdr server, so a headless castle is unaffected. Returns
    /// whether a notification was pushed.
    fn notify_escalation(&self, tuple: &Tuple) -> rk_core::Result<bool> {
        if !self.config.notify_escalations {
            return Ok(false);
        }
        if tuple.category != Category::Need || tuple.identity != STEWARD_ESCALATION_IDENTITY {
            return Ok(false);
        }
        let key = format!("notify-escalation@{}", tuple.id);
        if self.already_fired(&key)? {
            return Ok(false);
        }
        let task = tuple
            .payload
            .get("task")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let body = tuple
            .payload
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("a completed branch needs a human merge decision");
        rk_mux::HerdrMux::notify(&format!("steward escalation — {task}"), body);
        self.mark_notified(&key, tuple)?;
        info!(tuple = %tuple.id, task, "reactor pushed steward escalation notification");
        Ok(true)
    }

    /// Durable "already notified" marker for the built-in escalation push. It
    /// has no trigger/workflow of its own, so it writes the marker directly,
    /// sharing `MARKER_IDENTITY` + the `key` field so [`already_fired`] de-dups
    /// it exactly as it does a fired trigger.
    fn mark_notified(&self, key: &str, tuple: &Tuple) -> rk_core::Result<()> {
        let mut marker = Tuple::new(
            Category::Event,
            SYSTEM_SCOPE,
            MARKER_IDENTITY,
            REACTOR_INSTANCE,
            json!({
                "key": key,
                "kind": "notify-escalation",
                "tuple": tuple.id.to_string(),
            }),
        );
        marker.lifecycle = Lifecycle::Ephemeral;
        if self.config.marker_ttl_secs > 0 {
            marker.expires_at = Some(
                chrono::Utc::now() + chrono::Duration::seconds(self.config.marker_ttl_secs as i64),
            );
        }
        self.space.out(marker)
    }

    /// The ordered candidate trigger files this cycle: every global-dir
    /// definition (no source repo) then each registered repo's existing
    /// `.rk/triggers.cue` (source repo = that repo). Enumerated fresh each cycle
    /// — a cheap `readdir` + `exists`, not a `cue` shell-out — so an added or
    /// removed file changes the cache stamp and forces a reparse.
    fn trigger_files(&self, registry: &RepoRegistry) -> Vec<TriggerFile> {
        let mut files: Vec<TriggerFile> = rk_workflow::definitions(&self.layout.triggers_dir())
            .into_iter()
            .map(|file| (file, None))
            .collect();
        for repo in registry.list() {
            let file = repo.path.join(".rk").join("triggers.cue");
            if file.exists() {
                files.push((file, Some(repo.name.clone())));
            }
        }
        files
    }

    /// Parse the candidate trigger files, reusing the cached parse when none of
    /// their stamps changed. Only the reparse path shells out to `cue`.
    fn cached_triggers(&self, registry: &RepoRegistry) -> Vec<Loaded> {
        let files = self.trigger_files(registry);
        let stamps = file_stamps(&files);
        {
            let cache = self.trigger_cache.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(c) = cache.as_ref() {
                if c.stamps == stamps {
                    return c.triggers.clone();
                }
            }
        }
        let triggers = self.load_all_triggers(&files);
        let mut cache = self.trigger_cache.lock().unwrap_or_else(|p| p.into_inner());
        *cache = Some(TriggerCache {
            stamps,
            triggers: triggers.clone(),
        });
        triggers
    }

    /// Load and parse the given trigger files (the `cue` shell-out per file). A
    /// malformed file is logged and skipped, never fatal.
    fn load_all_triggers(&self, files: &[TriggerFile]) -> Vec<Loaded> {
        let mut out = Vec::new();
        for (file, source_repo) in files {
            match rk_workflow::load_triggers(file) {
                Ok(ts) => out.extend(ts.into_iter().map(|trigger| Loaded {
                    trigger,
                    source_repo: source_repo.clone(),
                })),
                Err(e) => {
                    warn!(file = %file.display(), error = %e, "reactor: bad trigger file")
                }
            }
        }
        out
    }

    fn excluded(&self, trigger: &Trigger, tuple: &Tuple) -> bool {
        tuple.instance == REACTOR_INSTANCE
            || self
                .config
                .exclude_instances
                .iter()
                .any(|e| e == &tuple.instance)
            || trigger.exclude.iter().any(|e| e == &tuple.instance)
    }

    /// Build the one authoritative [`Pattern`] for a trigger's match predicate,
    /// so the reactor uses the same match logic as every reader in the system.
    fn pattern(&self, trigger: &Trigger) -> rk_core::Result<Pattern> {
        let m = &trigger.matcher;
        let mut p = Pattern::default();
        if let Some(c) = &m.category {
            p.category = Some(Category::from_str(c)?);
        }
        p.scope = m.scope.clone();
        p.identity = m.identity.clone();
        p.instance = m.instance.clone();
        p.payload_search = m.search.clone();
        Ok(p)
    }

    fn already_fired(&self, key: &str) -> rk_core::Result<bool> {
        let mut p = Pattern::category(Category::Event)
            .scope(SYSTEM_SCOPE)
            .identity(MARKER_IDENTITY);
        p.payload_search = Some(format!("\"key\":\"{key}\""));
        Ok(!self.space.scan(&p)?.is_empty())
    }

    fn mark_fired(
        &self,
        key: &str,
        trigger: &Trigger,
        tuple: &Tuple,
        instance: &str,
    ) -> rk_core::Result<()> {
        let mut marker = Tuple::new(
            Category::Event,
            SYSTEM_SCOPE,
            MARKER_IDENTITY,
            REACTOR_INSTANCE,
            json!({
                "key": key,
                "trigger": trigger.name,
                "tuple": tuple.id.to_string(),
                "workflow": trigger.run,
                "instance": instance,
            }),
        );
        // Ephemeral with a TTL: the marker only needs to outlive any redelivery,
        // then self-collects. (Ephemeral tuples do not replicate — dedup is
        // per-castle, which is correct: each castle runs its own reactor.)
        marker.lifecycle = Lifecycle::Ephemeral;
        if self.config.marker_ttl_secs > 0 {
            marker.expires_at = Some(
                chrono::Utc::now() + chrono::Duration::seconds(self.config.marker_ttl_secs as i64),
            );
        }
        self.space.out(marker)
    }

    fn rate_limited(&self, trigger: &Trigger) -> bool {
        let cap = trigger.max_fires.unwrap_or(self.config.max_fires).min(100);
        if cap == 0 {
            return true;
        }
        let window = Duration::from_secs(self.config.window_secs.max(1));
        let now = Instant::now();
        let mut map = self.fires.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.entry(trigger.name.clone()).or_default();
        entry.retain(|t| now.duration_since(*t) < window);
        entry.len() as u32 >= cap
    }

    fn record_fire(&self, name: &str) {
        let mut map = self.fires.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(name.to_string())
            .or_default()
            .push(Instant::now());
    }

    /// Test-only: how many workflow instances the fired workflows created.
    #[doc(hidden)]
    pub fn engine_instance_count(&self) -> usize {
        self.engine.list().len()
    }

    fn load_cursor(&self) -> Option<RecordId> {
        std::fs::read_to_string(&self.cursor_file)
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    fn save_cursor(&self, id: RecordId) -> rk_core::Result<()> {
        std::fs::write(&self.cursor_file, id.to_string())?;
        Ok(())
    }
}

/// Stamp each candidate trigger file with `(mtime, len)` for change detection.
/// A missing/unreadable file stamps as `(None, None)` — appearing or vanishing
/// still flips the stamp, forcing a reparse. `metadata` is a cheap `stat`, never
/// a `cue` shell-out.
fn file_stamps(files: &[TriggerFile]) -> Vec<FileStamp> {
    files
        .iter()
        .map(|(path, repo)| {
            let meta = std::fs::metadata(path).ok();
            let mtime = meta.as_ref().and_then(|m| m.modified().ok());
            let len = meta.as_ref().map(|m| m.len());
            (path.clone(), repo.clone(), mtime, len)
        })
        .collect()
}

/// Normalise an obstacle/need report into a stable topic key: lowercase, keep
/// only alphanumeric runs as words, collapse to single spaces, and cap length.
/// Two rats phrasing the same wall slightly differently ("cargo build fails" vs
/// "Cargo build FAILS!!") land in the same bucket; length is bounded so the key
/// stays usable as an identity suffix and payload field.
fn normalize_topic(text: &str) -> String {
    let mut out = String::new();
    let mut prev_space = true; // leading: suppress a leading separator
    for c in text.chars() {
        if c.is_alphanumeric() {
            for lc in c.to_lowercase() {
                out.push(lc);
            }
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }
    let trimmed = out.trim_end();
    truncate(trimmed, 80).to_string()
}

/// Stable dedupe key for a coalesced topic: scope-qualified so the same wall in
/// two repos files two tickets, not one.
fn coalesce_key(scope: &str, topic: &str) -> String {
    format!("{scope}::{topic}")
}

/// A ticket is "done" (no longer a live dedupe guard) once closed.
fn ticket_is_done(t: &Tuple) -> bool {
    matches!(
        t.payload.get("status").and_then(Value::as_str),
        Some("done") | Some("closed")
    )
}

/// Compose the mid-session steer message for a newly promoted convention.
fn convention_steer_message(text: &str) -> String {
    format!(
        "📜 New standing convention in effect (promoted at quorum): {text}\n\
         This is now a binding fleet norm — apply it to your remaining work."
    )
}

/// The live rats a promoted convention should reach: every live agent when the
/// norm is system-scoped, or only agents in the norm's repo otherwise. A pure
/// selector over a registry snapshot so the scope logic is unit-testable without
/// a live session.
fn convention_steer_targets<'a>(agents: &'a [AgentRecord], scope: &str) -> Vec<&'a str> {
    agents
        .iter()
        .filter(|r| r.state.is_live())
        .filter(|r| scope == SYSTEM_SCOPE || r.repo_name == scope)
        .map(|r| r.name.as_str())
        .collect()
}

/// Truncate to at most `max` chars on a char boundary (byte-safe for UTF-8).
fn truncate(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// Template each workflow param from the matched tuple.
fn template_params(params: &HashMap<String, String>, tuple: &Tuple) -> HashMap<String, Value> {
    params
        .iter()
        .map(|(k, v)| (k.clone(), template_param(v, tuple)))
        .collect()
}

/// Render one param value. A lone whole-value `{{tuple.payload.<key>}}`
/// placeholder passes the raw JSON value through (preserving its type for the
/// workflow's typed params); anything else is string-interpolated.
fn template_param(raw: &str, tuple: &Tuple) -> Value {
    if let Some(rest) = raw.strip_prefix("{{tuple.payload.") {
        if let Some(key) = rest.strip_suffix("}}") {
            if !key.contains("{{") && !key.contains("}}") {
                return tuple.payload.get(key).cloned().unwrap_or(Value::Null);
            }
        }
    }
    Value::String(interpolate_tuple(raw, tuple))
}

fn interpolate_tuple(text: &str, tuple: &Tuple) -> String {
    let mut out = text
        .replace("{{tuple.category}}", tuple.category.as_str())
        .replace("{{tuple.scope}}", &tuple.scope)
        .replace("{{tuple.identity}}", &tuple.identity)
        .replace("{{tuple.instance}}", &tuple.instance)
        .replace("{{tuple.id}}", &tuple.id.to_string());
    if let Value::Object(map) = &tuple.payload {
        for (k, v) in map {
            out = out.replace(&format!("{{{{tuple.payload.{k}}}}}"), &scalar_str(v));
        }
    }
    out
}

fn scalar_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_workflow::TriggerMatch;

    fn tuple() -> Tuple {
        Tuple::new(
            Category::Endorsement,
            "system",
            "endorse",
            "Whisker",
            json!({"suggestion": "sug-1", "count": 3}),
        )
    }

    fn trigger(params: &[(&str, &str)]) -> Trigger {
        Trigger {
            name: "t".into(),
            matcher: TriggerMatch::default(),
            run: "w".into(),
            repo: None,
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            exclude: Vec::new(),
            max_fires: None,
        }
    }

    #[test]
    fn whole_value_payload_placeholder_preserves_type() {
        let t = tuple();
        let params = template_params(&trigger(&[("n", "{{tuple.payload.count}}")]).params, &t);
        // Passed through as the raw JSON number, not a string.
        assert_eq!(params["n"], json!(3));
    }

    #[test]
    fn string_interpolation_mixes_fields_and_payload() {
        let t = tuple();
        let params = template_params(
            &trigger(&[("label", "{{tuple.identity}}:{{tuple.payload.suggestion}}")]).params,
            &t,
        );
        assert_eq!(params["label"], json!("endorse:sug-1"));
    }

    #[test]
    fn missing_payload_key_is_null() {
        let t = tuple();
        let params = template_params(&trigger(&[("x", "{{tuple.payload.absent}}")]).params, &t);
        assert_eq!(params["x"], Value::Null);
    }

    #[test]
    fn tuple_structural_fields_interpolate() {
        let t = tuple();
        assert_eq!(
            interpolate_tuple("{{tuple.category}}/{{tuple.scope}}", &t),
            "endorsement/system"
        );
    }

    #[test]
    fn normalize_topic_folds_case_and_punctuation() {
        // Different phrasings of one wall collapse to the same topic key.
        assert_eq!(normalize_topic("Cargo build FAILS!!"), "cargo build fails");
        assert_eq!(normalize_topic("  cargo   build  fails  "), "cargo build fails");
        assert_eq!(
            normalize_topic("cargo-build: fails (rk-space)"),
            "cargo build fails rk space"
        );
    }

    #[test]
    fn normalize_topic_empty_when_no_words() {
        assert_eq!(normalize_topic("!!! ??? ..."), "");
        assert_eq!(normalize_topic("   "), "");
    }

    #[test]
    fn normalize_topic_is_length_bounded() {
        let long = "word ".repeat(50);
        assert!(normalize_topic(&long).chars().count() <= 80);
    }

    #[test]
    fn coalesce_key_is_scope_qualified() {
        assert_eq!(coalesce_key("repoA", "flaky test"), "repoA::flaky test");
        assert_ne!(
            coalesce_key("repoA", "flaky test"),
            coalesce_key("repoB", "flaky test"),
            "same wall in two repos is two keys"
        );
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 80), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // Multi-byte: must not split a codepoint.
        assert_eq!(truncate("héllo", 2), "hé");
    }

    fn agent(name: &str, repo: &str, state: crate::agents::AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            role: "rat".into(),
            harness: "fake".into(),
            model: None,
            repo_root: std::path::PathBuf::from("/tmp"),
            repo_name: repo.into(),
            task: None,
            branch: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            result: None,
            usage: Default::default(),
            cost_usd: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            archived_at: None,
        }
    }

    #[test]
    fn steer_message_carries_the_norm_text() {
        let msg = convention_steer_message("always run cargo fmt");
        assert!(msg.contains("always run cargo fmt"));
        assert!(msg.contains("binding fleet norm"));
    }

    #[test]
    fn system_scope_convention_reaches_every_live_rat_across_repos() {
        use crate::agents::AgentState::*;
        let agents = [
            agent("Whisker", "repoA", Running),
            agent("Nibbles", "repoB", Running),
            agent("Gone", "repoA", Completed),
        ];
        let targets = convention_steer_targets(&agents, SYSTEM_SCOPE);
        // Both live rats, regardless of repo; the completed rat is excluded.
        assert_eq!(targets, vec!["Whisker", "Nibbles"]);
    }

    #[test]
    fn repo_scoped_convention_reaches_only_that_repos_live_rats() {
        use crate::agents::AgentState::*;
        let agents = [
            agent("Whisker", "repoA", Running),
            agent("Nibbles", "repoB", Running),
            agent("Spawning", "repoA", Spawning),
        ];
        let targets = convention_steer_targets(&agents, "repoA");
        assert_eq!(targets, vec!["Whisker", "Spawning"]);
    }

    #[test]
    fn no_live_rats_means_no_steer_targets() {
        use crate::agents::AgentState::*;
        let agents = [
            agent("Gone", "repoA", Completed),
            agent("Dead", "repoA", Failed),
            agent("Orphan", "repoA", Orphaned),
        ];
        assert!(convention_steer_targets(&agents, SYSTEM_SCOPE).is_empty());
    }
}
