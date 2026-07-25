#!/usr/bin/env python3
"""TKT-148 — audit an RK_HOME for agent-name ambiguity.

TKT-136 briefly narrowed `Registry::reserve_name` to the LIVE map, so archiving
a rat returned its name to the pool. TKT-146 (afaa8a0) restored the invariant
that a name is an identity key and is never recycled, but it could not retract
the records the recycled names were already stamped into. This script measures
what that window actually left behind, so the cleanup decision is made against
data rather than against the estimate in the ticket.

It reports four things, each of which is a distinct kind of name ambiguity:

  1. DUPLICATED GENERATIONS — names carrying more than one agent record across
     `agents.json` + `agents-archive.json`. This is the TKT-136 residue proper.
     TKT-146 means the set cannot grow; it can only shrink (never, in practice,
     since nothing deletes records).

  2. INTERLEAVED TRANSCRIPTS — of those names, the ones whose
     `agent-logs/<name>.jsonl` actually holds entries from more than one
     generation. `AgentLog::path_for` keys the file on the NAME ALONE
     (crates/rk-daemon/src/agent_log.rs), so this is possible in principle; it
     is the stated motivation for the cleanup and is worth checking rather than
     assuming. An entry is attributed to the newest generation whose
     `created_at` precedes it.

  3. DUPLICATE NAME-KEYED DURABLE TUPLES — (identity, agent) pairs carrying more
     than one durable tuple. This is what actually broke TKT-146: a blocking
     read keyed on an agent name over a durable category resolves
     `ORDER BY id ASC LIMIT 1`, so it takes the OLDEST match.

  4. MULTI-RESULT SINGLE GENERATIONS — the subset of (3) where the duplicate
     tuples belong to ONE generation. These are NOT TKT-136 residue and are NOT
     fixed by bounding a read to the agent's own `created_at`: a harness that
     returns control more than once (a re-armed monitor, a task notification)
     emits a `harness_result` per turn, and the oldest can be mid-flight.

Usage:
    ./scripts/rk-name-generations-audit.py [RK_HOME] [--json]

RK_HOME defaults to $RK_HOME, else ~/.rat-kingdom. Read-only: it opens the
space in immutable mode and never writes. Safe to run against a live fleet.
"""

import argparse
import collections
import json
import os
import pathlib
import sqlite3
import sys
from datetime import datetime

# Durable, name-keyed categories a blocking read is known to key on.
NAME_KEYED_IDENTITIES = ("harness_result", "task_done")


def parse_ts(s):
    """Parse the RFC3339 stamps the daemon writes, Z-suffixed or offset."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_records(home):
    """Every agent record, live and archived, as (name, source, record).

    `agents.json` is a map keyed by name; `agents-archive.json` is a Vec keyed
    by GENERATION (name, created_at), because TKT-136 had to let several
    generations of one name coexist.
    """
    records = []
    live_path = home / "agents.json"
    if live_path.exists():
        for name, rec in json.loads(live_path.read_text()).items():
            records.append((rec.get("name", name), "live", rec))
    archive_path = home / "agents-archive.json"
    if archive_path.exists():
        for rec in json.loads(archive_path.read_text()):
            records.append((rec["name"], "archived", rec))
    return records


def duplicated_generations(records):
    """Names carrying more than one record, oldest generation first."""
    by_name = collections.defaultdict(list)
    for name, source, rec in records:
        by_name[name].append((source, rec))
    return {
        name: sorted(gens, key=lambda g: g[1]["created_at"])
        for name, gens in by_name.items()
        if len(gens) > 1
    }


def transcript_split(home, name, generations):
    """Attribute a name's log entries to generations.

    Returns (total_entries, {generation_index: count}), where index -1 collects
    entries predating every generation (only possible if a record was rewritten).
    A name with no log file returns (0, {}).
    """
    path = home / "agent-logs" / f"{name}.jsonl"
    if not path.exists():
        return 0, {}
    starts = [parse_ts(rec["created_at"]) for _, rec in generations]
    counts = collections.Counter()
    total = 0
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ts = parse_ts(json.loads(line)["ts"])
        except (ValueError, KeyError, json.JSONDecodeError):
            continue  # torn write; AgentLog::read skips these too
        total += 1
        owner = max((i for i, s in enumerate(starts) if s <= ts), default=-1)
        counts[owner] += 1
    return total, dict(counts)


def name_keyed_tuples(home):
    """(identity, agent) -> [(created_at, scope, id)] over durable categories."""
    db_path = home / "space.db"
    if not db_path.exists():
        return {}
    # Immutable: never take a write lock on a live fleet's space.
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    try:
        placeholders = ",".join("?" * len(NAME_KEYED_IDENTITIES))
        rows = conn.execute(
            f"SELECT id, scope, identity, payload, created_at FROM tuples "
            f"WHERE identity IN ({placeholders})",
            NAME_KEYED_IDENTITIES,
        ).fetchall()
    finally:
        conn.close()

    by_key = collections.defaultdict(list)
    for tuple_id, scope, identity, payload, created_at in rows:
        try:
            agent = json.loads(payload).get("agent")
        except json.JSONDecodeError:
            continue
        if agent:
            by_key[(identity, agent)].append((created_at, scope, tuple_id))
    return {k: sorted(v) for k, v in by_key.items()}


def generation_of(created_at, generations):
    """Index of the newest generation preceding `created_at`, or -1."""
    ts = parse_ts(created_at)
    starts = [parse_ts(rec["created_at"]) for _, rec in generations]
    return max((i for i, s in enumerate(starts) if s <= ts), default=-1)


def audit(home):
    records = load_records(home)
    dups = duplicated_generations(records)

    interleaved, single_gen_log, no_log = [], [], []
    for name, generations in sorted(dups.items()):
        total, counts = transcript_split(home, name, generations)
        owners = [i for i in counts if i >= 0]
        if not total:
            no_log.append(name)
        elif len(owners) > 1:
            interleaved.append({"name": name, "entries": total, "per_generation": counts})
        else:
            single_gen_log.append({"name": name, "entries": total})

    tuples = name_keyed_tuples(home)
    multi = {k: v for k, v in tuples.items() if len(v) > 1}

    # Classify the duplicate tuples on two INDEPENDENT axes, because a name can
    # be both: straddling generations is TKT-136 residue, while several tuples
    # inside ONE generation is a rat emitting several results, which no
    # name-uniqueness fix addresses. `Rizzo` is both.
    cross_generation, within_generation = [], []
    for (identity, agent), entries in sorted(multi.items()):
        generations = dups.get(agent)
        per_generation = collections.Counter(
            generation_of(c, generations) if generations else 0 for c, _, _ in entries
        )
        row = {
            "identity": identity,
            "agent": agent,
            "count": len(entries),
            "per_generation": dict(per_generation),
            "tuples": [{"created_at": c, "scope": s, "id": i} for c, s, i in entries],
        }
        if len(per_generation) > 1:
            cross_generation.append(row)
        if max(per_generation.values()) > 1:
            within_generation.append(row)

    return {
        "home": str(home),
        "records": len(records),
        "distinct_names": len({n for n, _, _ in records}),
        "duplicated_generations": {
            name: [
                {
                    "source": source,
                    "created_at": rec["created_at"],
                    "state": rec.get("state"),
                    "repo": rec.get("repo_name"),
                    "task": rec.get("task"),
                }
                for source, rec in generations
            ]
            for name, generations in sorted(dups.items())
        },
        "interleaved_transcripts": interleaved,
        "single_generation_transcripts": single_gen_log,
        "names_without_transcript": no_log,
        "duplicate_tuples_cross_generation": cross_generation,
        "duplicate_tuples_within_generation": within_generation,
    }


def render(report):
    dups = report["duplicated_generations"]
    print(f"RK_HOME: {report['home']}")
    print(f"  agent records ........ {report['records']}")
    print(f"  distinct names ....... {report['distinct_names']}")
    print()

    print(f"[1] DUPLICATED NAME GENERATIONS: {len(dups)}")
    for name, generations in dups.items():
        print(f"  {name}")
        for g in generations:
            print(
                f"      {g['source']:9} {g['created_at']}  "
                f"{str(g['state']):10} {str(g['repo']):12} {g['task']}"
            )
    print()

    interleaved = report["interleaved_transcripts"]
    print(f"[2] INTERLEAVED TRANSCRIPTS: {len(interleaved)}")
    for row in interleaved:
        print(f"  {row['name']:14} {row['entries']} entries  per-gen={row['per_generation']}")
    if not interleaved:
        print("  none — no `rk log <name>` mixes two generations")
    print(
        f"  ({len(report['single_generation_transcripts'])} duplicated names have a "
        f"single-generation log, {len(report['names_without_transcript'])} have none)"
    )
    print()

    cross = report["duplicate_tuples_cross_generation"]
    print(f"[3] DUPLICATE NAME-KEYED TUPLES, CROSS-GENERATION: {len(cross)}")
    for row in cross:
        print(f"  {row['identity']:15} {row['agent']:14} x{row['count']}")
    print()

    within = report["duplicate_tuples_within_generation"]
    print(f"[4] MULTI-RESULT SINGLE GENERATIONS: {len(within)}")
    for row in within:
        worst = max(row["per_generation"].values())
        print(f"  {row['identity']:15} {row['agent']:14} x{worst} within one generation")
    if within:
        print(
            "  NOTE: these are one generation emitting several results. Bounding a\n"
            "  read to the agent's own created_at does NOT disambiguate them — the\n"
            "  oldest match can be a mid-flight turn."
        )


def main():
    parser = argparse.ArgumentParser(description="Audit an RK_HOME for agent-name ambiguity.")
    parser.add_argument("home", nargs="?", help="RK_HOME (default $RK_HOME, else ~/.rat-kingdom)")
    parser.add_argument("--json", action="store_true", help="emit the raw report as JSON")
    args = parser.parse_args()

    home = pathlib.Path(
        args.home or os.environ.get("RK_HOME") or pathlib.Path.home() / ".rat-kingdom"
    ).expanduser()
    if not home.is_dir():
        sys.exit(f"no such RK_HOME: {home}")

    report = audit(home)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        render(report)


if __name__ == "__main__":
    main()
