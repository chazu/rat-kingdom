#!/usr/bin/env python3
"""Preview or apply installed landing terminology changes, preserving customization."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path


def rename(text):
    for old, new in (
        ("steward-landing-on-completion", "landing-on-completion"),
        ("steward-on-completion", "legacy-landing-on-completion"),
        ("steward-review", "candidate-review"),
        ("steward_review", "candidate_review"),
        ("Steward", "Landing"),
        ("STEWARD", "LANDING"),
        ("steward", "landing"),
    ):
        text = text.replace(old, new)
    return text


def plan(home, repos):
    paths = {home / "config.toml"}
    for root in [home, *(repo / ".rk" for repo in repos)]:
        paths.update(root.glob("*.cue"))
        for subdir in ("workflows", "triggers", "schedules"):
            paths.update((root / subdir).glob("*.cue"))
    changes = []
    destinations = set()
    for path in sorted(paths):
        if not path.is_file():
            continue
        original = path.read_text()
        updated = rename(original)
        target = path.with_name(rename(path.name))
        if updated == original and target == path:
            continue
        if target in destinations or (target != path and target.exists()):
            raise ValueError(f"refusing conflicting destination: {target}")
        if path.is_symlink():
            raise ValueError(f"refusing to replace symlink: {path}")
        destinations.add(target)
        changes.append((path, target, original, updated))
    return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, default=Path(os.environ.get("RK_HOME", Path.home() / ".rat-kingdom")))
    parser.add_argument("--repo", type=Path, action="append", default=[])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    changes = plan(args.home.resolve(), [repo.resolve() for repo in args.repo])
    for source, target, _, _ in changes:
        print(f"{source} -> {target}")
    if not args.apply:
        print(f"Preview: {len(changes)} files; pass --apply to write with backups.")
        return
    if not changes:
        print("Already migrated.")
        return
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = args.home / "name-migrations" / stamp
    backup.mkdir(parents=True, exist_ok=False)
    manifest = []
    # Back up the entire validated plan before changing any installed file.
    for source, target, original, _ in changes:
        name = hashlib.sha256(str(source).encode()).hexdigest()
        (backup / name).write_text(original)
        manifest.append({"source": str(source), "target": str(target), "backup": name})
    (backup / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for source, target, original, updated in changes:
        if source.read_text() != original:
            raise RuntimeError(f"source changed during migration: {source}; backups: {backup}")
        temporary = target.with_name(target.name + ".rename.tmp")
        with temporary.open("x") as stream:
            stream.write(updated)
        temporary.chmod(source.stat().st_mode)
        os.replace(temporary, target)
        if source != target:
            source.unlink()
    print(f"Migrated {len(changes)} files. Backups: {backup}")
    print("Reload the daemon before the next ticket trial.")


if __name__ == "__main__":
    main()
