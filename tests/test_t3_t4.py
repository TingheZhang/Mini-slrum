import os
import sys
import subprocess
import tempfile
import pathlib
import time
import json
import sqlite3
import re
from pathlib import Path

BASE = Path('/home/lisy/mini-slurm')
EXE = Path('/mnt/share/mini-slurm/libexec/mslurm-executor')
sys.path.insert(0, str(BASE / 'lib'))
import mslurm_db as dbmod

with tempfile.TemporaryDirectory(prefix='test-t3-t4-') as td:
    root = Path(td)

    # 1. Test T3: Early SIGTERM
    work = root / 'early-term'
    work.mkdir()
    (work / 'job.sh').write_text('echo ran > ran\nsleep 30\n')
    boot = '''import os,sys,signal,subprocess,runpy,pathlib,time
original = subprocess.Popen
def interrupted_start(*args, **kwargs):
    p = original(*args, **kwargs)
    pathlib.Path('spawned.pid').write_text(str(p.pid))
    deadline=time.monotonic()+2
    while not pathlib.Path('ran').exists() and time.monotonic()<deadline:
        time.sleep(0.01)
    os.kill(os.getpid(), signal.SIGTERM)
    return p
subprocess.Popen = interrupted_start
sys.argv[0]='/mnt/share/mini-slurm/libexec/mslurm-executor'
runpy.run_path(sys.argv[0], run_name='__main__')
'''
    args = [sys.executable, '-B', '-c', boot, '--job-id', '918273647',
            '--script', str(work / 'job.sh'), '--chdir', str(work),
            '--stdout', str(work / 'out'), '--stderr', str(work / 'err'),
            '--gpus', '', '--timeout', '5']
    res = subprocess.run(args, capture_output=True, text=True, timeout=10)
    child_pid = int((work / 'spawned.pid').read_text())
    time.sleep(0.5)
    stat = Path('/proc') / str(child_pid) / 'stat'
    child_alive = stat.exists() and stat.read_text().split(') ', 1)[1][0] != 'Z'
    status_exists = (work / '.status.json').exists()
    status_data = json.loads((work / '.status.json').read_text()) if status_exists else {}

    print(f"T3 early-term: exit={res.returncode}, child_alive={child_alive}, status_exists={status_exists}, status={status_data.get('status')}")
    assert not child_alive, f"Child process must be killed, but child_alive={child_alive}"
    assert status_exists, "Status file must be written"
    assert status_data.get('status') == 'CANCELLED', f"Expected CANCELLED, got {status_data.get('status')}"
    print("T3 EARLY SIGTERM TEST PASSED: Child killed cleanly, status written as CANCELLED!")

    # 2. Test T4: Legacy Migration with Invalid Historical Data
    legacy = re.sub(r' CHECK \([^)]*\)', '', dbmod.SCHEMA)
    path = root / 'legacy.db'
    with sqlite3.connect(path) as con:
        con.executescript(legacy)
        con.execute("INSERT INTO jobs (name,req_cpus) VALUES ('valid_history',1)")
        con.execute("INSERT INTO jobs (name,req_cpus) VALUES ('old_bad_value',-8)")
    
    # Initialize MiniSlurmDB on legacy DB containing -8
    migrated_db = dbmod.MiniSlurmDB(str(path))
    with sqlite3.connect(path) as con:
        rows = con.execute("SELECT name, req_cpus FROM jobs ORDER BY job_id").fetchall()
        jobs_count = len(rows)
        old_exists = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_jobs_old'").fetchone()

    print(f"T4 migration: jobs_count={jobs_count}, rows={rows}, _jobs_old_remains={bool(old_exists)}")
    assert jobs_count == 2, f"Expected 2 jobs migrated, got {jobs_count}"
    assert rows[0] == ('valid_history', 1), f"Row 0 mismatch: {rows[0]}"
    assert rows[1] == ('old_bad_value', 1), f"Row 1 should be normalized to 1, got: {rows[1]}"
    assert not old_exists, "_jobs_old must be cleaned up after successful migration"
    print("T4 MIGRATION TEST PASSED: Legacy invalid data successfully normalized, zero records lost!")

print("ALL T3 & T4 TESTS PASSED SUCCESSFULLY!")
