//! Single-writer, incremental observation log. Checkpoints are disposable
//! caches: replaying complete samples is sufficient to reconstruct them.

use super::{
    advance_sample_progress, load_interventions, ready_ticket_ids, write_json_atomic, Manifest,
    ProgressMetrics, ProgressState, Sample, Thresholds, SAMPLES,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

const CHECKPOINT: &str = "collector.json";

#[derive(Debug, Default, Serialize, Deserialize)]
struct Checkpoint {
    manifest_digest: String,
    #[serde(default)]
    evaluator_version: u32,
    offset: u64,
    last_start: u64,
    last_digest: String,
    sequence: u64,
    last_observed: Option<DateTime<Utc>>,
    event_cursor: Option<String>,
    ready_since: BTreeMap<String, DateTime<Utc>>,
    /// D1's independent progress evaluator state, keyed by ticket identity.
    /// Persisted (never just recomputed from the tail) so a restart resumes
    /// the same stall clock and generation binding instead of granting a
    /// fresh grace window — see `advance_progress_state`.
    #[serde(default)]
    progress: BTreeMap<String, ProgressState>,
}

pub(super) struct ObservationLog {
    run: PathBuf,
    file: File,
    state: Checkpoint,
    recoveries: Vec<String>,
    thresholds: Thresholds,
    #[cfg(test)]
    pub replayed_samples: usize,
}

impl ObservationLog {
    pub fn open(run: &Path, manifest: &Manifest) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(run.join(SAMPLES))?;
        // SAFETY: the descriptor is live and owned for the lifetime of this
        // log. Closing File releases the lock even after process termination.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("another collector owns this observation run");
        }
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(manifest)?));
        let cached = fs::read(run.join(CHECKPOINT))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Checkpoint>(&bytes).ok());
        let state = match cached {
            Some(state)
                if state.manifest_digest == digest
                    && state.evaluator_version == super::PROGRESS_EVALUATOR_VERSION
                    && valid_checkpoint(&mut file, &state)? =>
            {
                state
            }
            _ => Checkpoint {
                manifest_digest: digest,
                evaluator_version: super::PROGRESS_EVALUATOR_VERSION,
                ..Default::default()
            },
        };
        let mut log = Self {
            run: run.into(),
            file,
            state,
            recoveries: Vec::new(),
            thresholds: manifest.thresholds.clone(),
            #[cfg(test)]
            replayed_samples: 0,
        };
        log.replay_tail()?;
        let recovery_dir = run.join("recovery");
        if recovery_dir.exists() {
            for entry in fs::read_dir(recovery_dir)? {
                let path = entry?.path();
                if path.extension().is_some_and(|ext| ext == "partial") {
                    log.recoveries.push(format!(
                        "recovery/{}",
                        path.file_name().unwrap().to_string_lossy()
                    ));
                }
            }
            log.recoveries.sort();
        }
        log.checkpoint()?;
        Ok(log)
    }

    pub fn next_sequence(&self) -> u64 {
        self.state.sequence + 1
    }
    pub fn event_cursor(&self) -> Option<&str> {
        self.state.event_cursor.as_deref()
    }
    pub fn recoveries(&self) -> &[String] {
        &self.recoveries
    }
    pub fn ready_age(&self, ticket: &str, now: DateTime<Utc>) -> u64 {
        self.state
            .ready_since
            .get(ticket)
            .and_then(|since| now.signed_duration_since(*since).to_std().ok())
            .map_or(0, |age| age.as_secs())
    }
    /// Preview without advancing the cache before the sample is durable.
    /// Reloads declared-intervention evidence fresh each call: it lives in
    /// its own append-only directory, not the cached checkpoint, so a gate
    /// declared between samples is picked up without a restart.
    pub fn progress_metrics(&self, sample: &Sample) -> Result<ProgressMetrics> {
        let interventions = load_interventions(&self.run)?;
        let mut states = self.state.progress.clone();
        Ok(advance_sample_progress(
            &mut states,
            sample,
            &self.thresholds,
            &interventions,
        ))
    }
    pub fn gap(&self, start: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
        now.signed_duration_since(self.state.last_observed.unwrap_or(start))
            .to_std()
            .map_or(0, |gap| gap.as_secs())
    }

    pub fn append(&mut self, sample: &Sample) -> Result<()> {
        if sample.sequence != self.next_sequence() {
            bail!("observation sequence changed before append");
        }
        if self.file.metadata()?.len() != self.state.offset {
            bail!(
                "observation log changed outside its collector; reopen to replay before appending"
            );
        }
        let mut bytes = serde_json::to_vec(sample)?;
        bytes.push(b'\n');
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        self.absorb(sample, &bytes)?;
        self.checkpoint()?;
        self.recoveries.clear();
        Ok(())
    }

    fn absorb(&mut self, sample: &Sample, bytes: &[u8]) -> Result<()> {
        let interventions = load_interventions(&self.run)?;
        let ready = ready_ticket_ids(sample);
        self.state.ready_since.retain(|id, _| ready.contains(id));
        for id in ready {
            self.state
                .ready_since
                .entry(id)
                .or_insert(sample.observed_at);
        }
        advance_sample_progress(
            &mut self.state.progress,
            sample,
            &self.thresholds,
            &interventions,
        );
        if let Some(cursor) = &sample.event_cursor {
            if self
                .state
                .event_cursor
                .as_ref()
                .is_none_or(|previous| cursor > previous)
            {
                self.state.event_cursor = Some(cursor.clone());
            }
        }
        self.state.sequence = sample.sequence;
        self.state.last_observed = Some(sample.observed_at);
        self.state.last_start = self.state.offset;
        self.state.offset += bytes.len() as u64;
        self.state.last_digest = hex::encode(Sha256::digest(bytes));
        Ok(())
    }

    fn replay_tail(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(self.state.offset))?;
        let mut reader = BufReader::new(self.file.try_clone()?);
        loop {
            let mut bytes = Vec::new();
            if reader.read_until(b'\n', &mut bytes)? == 0 {
                break;
            }
            let complete = bytes.ends_with(b"\n");
            if complete && bytes.iter().all(u8::is_ascii_whitespace) {
                self.state.last_start = self.state.offset;
                self.state.offset += bytes.len() as u64;
                self.state.last_digest = hex::encode(Sha256::digest(&bytes));
                continue;
            }
            let parsed = serde_json::from_slice::<Sample>(&bytes);
            if !complete {
                // Preserve every interrupted byte before repairing the final
                // append. Complete, committed samples are never rewritten.
                let recovery = self.run.join("recovery");
                fs::create_dir_all(&recovery)?;
                let fragment = recovery.join(format!(
                    "{}-{}.partial",
                    self.state.offset,
                    hex::encode(Sha256::digest(&bytes))
                ));
                if !fragment.exists() {
                    let mut saved = OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&fragment)?;
                    saved.write_all(&bytes)?;
                    saved.sync_all()?;
                }
                if parsed.is_ok() {
                    self.file.seek(SeekFrom::End(0))?;
                    self.file.write_all(b"\n")?;
                    bytes.push(b'\n');
                } else {
                    self.file.set_len(self.state.offset)?;
                    self.file.sync_data()?;
                    break;
                }
                self.file.sync_data()?;
            }
            let sample = parsed
                .with_context(|| format!("parse observation log at byte {}", self.state.offset))?;
            if sample.schema_version != super::SCHEMA_VERSION
                || sample.sequence != self.next_sequence()
            {
                bail!(
                    "invalid observation schema or sequence at byte {}",
                    self.state.offset
                );
            }
            self.absorb(&sample, &bytes)?;
            #[cfg(test)]
            {
                self.replayed_samples += 1;
            }
            if !complete {
                break;
            }
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        write_json_atomic(&self.run.join(CHECKPOINT), &self.state)
    }
}

fn valid_checkpoint(file: &mut File, state: &Checkpoint) -> Result<bool> {
    if state.offset == 0 {
        return Ok(state.sequence == 0);
    }
    let len = state.offset.saturating_sub(state.last_start);
    if state.offset > file.metadata()?.len() || len == 0 || len > 64 * 1024 * 1024 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(state.last_start))?;
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes)?;
    Ok(bytes.ends_with(b"\n") && hex::encode(Sha256::digest(&bytes)) == state.last_digest)
}
