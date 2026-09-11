#!/usr/bin/env python3
"""Compare identical managed checks with admission off/on; retain every outcome.

Uses isolated repositories and daemon homes. No worker/provider is dispatched.
The workload command is trusted operator input, executed as a named repo check.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

STRIP = ('RK_AGENT RK_TASK RK_REPO RK_ROLE RK_HOME RK_BRANCH RK_WORKTREE RK_AUTH_TOKEN '
         'RK_REVIEW_BRANCH RK_REVIEW_HEAD RK_REVIEW_TARGET RK_REVIEW_TASK RK_REVIEW_ATTEMPT').split()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rk', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--limit', type=int, default=1)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or min(args.jobs, args.rounds, args.limit) < 1:
        parser.error('provide a command and positive jobs, rounds, limit')
    rk = args.rk.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    base_env = {k: v for k, v in os.environ.items() if k not in STRIP}
    results = []
    for limit in (0, args.limit):
        case = output / f'limit-{limit}'
        repo, home = case / 'load-probe', case / 'home'
        repo.mkdir(parents=True)
        home.mkdir()
        env = dict(base_env, RK_HOME=str(home))
        (home / 'config.toml').write_text(
            f'[disk]\nmin_free_gb = 0\n[supervisor]\nenabled = false\n'
            f'[drain]\nenabled = false\n[policy]\nverification_admission_limit = {limit}\n')
        (repo / '.rk').mkdir()
        (repo / '.rk/repo.cue').write_text('repo: {}\n')
        marker = case / 'markers'
        marker.mkdir()
        # Each child leaves a unique marker over the real workload's lifetime.
        # Retain starts/finishes to measure occupancy independently of RK counters.
        script = case / 'workload.sh'
        script.write_text('#!/bin/sh\n' +
            f'marker={shlex.quote(str(marker))}/$$\n' +
            'touch "$marker"\ntrap \'rm -f "$marker"\' EXIT\n' +
            f'ls {shlex.quote(str(marker))} | wc -l >> {shlex.quote(str(case / "occupancy.txt"))}\n' +
            shlex.join(command) + '\n')
        # A distinct name per invocation prevents proof-cache hits.
        checks = [dict(name=f'load-{r}-{j}', command='sh '+shlex.quote(str(script)), timeout='240s',
                       environmentPolicy='strip_rk_spawn', sharedCargoTarget=False)
                  for r in range(args.rounds) for j in range(args.jobs)]
        checks += [dict(name='diagnostic-control', command='echo diagnostic-control >&2; exit 7',
                       timeout='30s', environmentPolicy='strip_rk_spawn', sharedCargoTarget=False)]
        (repo / '.rk/checks.cue').write_text(json.dumps({'checks': checks}))
        for argv in (['init','-b','main'], ['add','.'],
                     ['-c','user.name=Load Probe','-c','user.email=probe@example.invalid',
                      'commit','-m','isolated load fixture']):
            subprocess.run(['git','-C',str(repo),*argv],check=True,capture_output=True)
        def call(argv, timeout=300):
            return subprocess.run([str(rk),'--json',*argv],env=env,capture_output=True,timeout=timeout)
        def read(argv):
            p = call(argv)
            if p.returncode:
                raise RuntimeError(p.stderr.decode(errors='replace'))
            return json.loads(p.stdout)
        records = []
        try:
            read(['repo','add',str(repo)])
            for round_id in range(args.rounds):
                def one(job):
                    started = time.monotonic()
                    p = call(['verify','--repo','load-probe','--check',f'load-{round_id}-{job}'])
                    stem = case / f'round-{round_id}-job-{job}'
                    stem.with_suffix('.stdout').write_bytes(p.stdout)
                    stem.with_suffix('.stderr').write_bytes(p.stderr)
                    return dict(round=round_id,job=job,exit=p.returncode,
                                seconds=round(time.monotonic()-started,3))
                with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
                    records.extend(pool.map(one, range(args.jobs)))
            control = call(['verify','--repo','load-probe','--check','diagnostic-control'])
            (case / 'control.stdout').write_bytes(control.stdout)
            (case / 'control.stderr').write_bytes(control.stderr)
            failures = read(['scan','artifact','load-probe','gate-failure'])
            events = read(['scan','event','load-probe','verification_admission'])
            (case / 'gate-failures.json').write_text(json.dumps(failures,indent=2)+'\n')
            (case / 'admission-events.json').write_text(json.dumps(events,indent=2)+'\n')
            payloads = [t['payload'] for t in failures['tuples']]
            assert control.returncode == 7
            assert any(p['exit'] == 7 and 'diagnostic-control' in p['stderr_tail'] for p in payloads)
            assert len(payloads) == 1 + sum(r['exit'] != 0 for r in records), 'missing failure evidence'
            assert not failures.get('truncated') and not events.get('truncated')
            occupancy = list(map(int,(case/'occupancy.txt').read_text().split()))
            assert len(occupancy) == args.jobs * args.rounds, 'cached or missing execution'
            peak = max(occupancy)
            if limit:
                assert peak <= limit, (peak,limit)
            result = dict(limit=limit,runs=records,peak=peak,
                          failed=sum(r['exit'] != 0 for r in records),diagnostic_control_exit=control.returncode,
                          persisted_failures=len(payloads))
            results.append(result)
            (case / 'results.json').write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps(result),flush=True)
        finally:
            # The fixture has no agents. Stop only the daemon owning this home.
            try:
                call(['daemon','stop'], timeout=15)
            except subprocess.TimeoutExpired:
                pid_file = home/'rk.pid'
                if pid_file.exists():
                    os.kill(int(pid_file.read_text().strip()),9)
    report = dict(command=command,rk=str(rk),rk_sha256=hashlib.sha256(rk.read_bytes()).hexdigest(),
                  jobs=args.jobs,rounds=args.rounds,results=results)
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    # Red workloads remain red, regardless of the comparison's direction.
    return int(any(case['failed'] for case in results))


if __name__ == '__main__':
    raise SystemExit(main())
