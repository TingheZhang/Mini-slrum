"""Comprehensive Safety Verification for Mini-Slurm Fixes on 174.

Tests all 6 P1 and 4 P2 expert audit items against the deployed code on 174.
Ensures inverted assertions: all security and reliability constraints hold.
"""
import concurrent.futures
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import types
from unittest.mock import patch, mock_open

BASE = Path('/home/lisy/mini-slurm')
EXECUTOR = Path('/mnt/share/mini-slurm/libexec/mslurm-executor')

def load(path):
    module = types.ModuleType('verified_' + path.name.replace('.', '_'))
    module.__file__ = str(path)
    exec(compile(path.read_text(), str(path), 'exec'), module.__dict__)
    return module

dbmod = load(BASE / 'lib/mslurm_db.py')
sys.modules['mslurm_db'] = dbmod
parser = load(BASE / 'lib/mslurm_parser.py')

def emit(label, passed, **kwargs):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {label}: {json.dumps(kwargs, ensure_ascii=False)}", flush=True)

def setup(root, name, cpus=4, gpus=0):
    db = dbmod.MiniSlurmDB(str(root / name / 'test.db'))
    cfg = {'test': {'cpus': cpus, 'mem_mb': 4000,
                    'gpus': [{'id': i, 'model': 'test'} for i in range(gpus)]}}
    db.sync_nodes(cfg)
    return db, cfg

def submit(db, cpus=2, mem=1000, gpus=0):
    return db.submit_job('audit', 'audit', cpus, mem, gpus, None,
                         10, '/tmp', 'exit 0', None, None)

class EndLoop(BaseException):
    pass

def scheduler_case(job, sdata, ssh_code, now=1000):
    os.environ["MSLURM_NO_LOCK"] = "1"
    scheduler = load(BASE / 'sbin/mslurm-sched')
    finished = []
    fake_db = types.SimpleNamespace(
        sync_nodes=lambda cfg: None,
        list_jobs=lambda **kwargs: [job],
        get_pending_jobs=lambda: [],
        finish_job=lambda *args: finished.append(args),
        backup=lambda path: None,
        mark_job_running=lambda *args: None,
    )
    scheduler.MiniSlurmDB = lambda path: fake_db
    scheduler.load_nodes_config = lambda path: {}
    scheduler.time = types.SimpleNamespace(
        time=lambda: now,
        sleep=lambda delay: (_ for _ in ()).throw(EndLoop()),
    )
    scheduler.os = types.SimpleNamespace(path=types.SimpleNamespace(
        join=os.path.join, exists=lambda path: sdata is not None))
    scheduler.open = lambda *args, **kwargs: io.StringIO(json.dumps(sdata))
    scheduler.subprocess = types.SimpleNamespace(
        run=lambda *args, **kwargs: types.SimpleNamespace(returncode=ssh_code))
    try:
        scheduler.main()
    except EndLoop:
        pass
    return finished

def run_all_tests():
    print("=== Starting Mini-Slurm Fix Verification on 174 ===", flush=True)
    with tempfile.TemporaryDirectory(prefix='mslurm-verify-') as td:
        root = Path(td)

        # 1. [P1] finish_job Idempotency
        db, _ = setup(root, 'double-release')
        a, b = submit(db), submit(db)
        db.try_allocate_job(a)
        db.try_allocate_job(b)
        db.finish_job(a, 'COMPLETED', 0)
        before = db.list_nodes()[0]['allocated_cpus']
        # Second finish call must be no-op
        db.finish_job(a, 'COMPLETED', 0)
        after = db.list_nodes()[0]['allocated_cpus']
        c = submit(db, cpus=4)
        allocated = db.try_allocate_job(c)
        p1_pass = (before == 2 and after == 2 and allocated is None)
        assert p1_pass, f"Expected (2, 2, None), got ({before}, {after}, {allocated})"
        emit("P1_finish_job_idempotency", p1_pass, allocated_cpus_before=before, allocated_cpus_after=after, overcommit_prevented=(allocated is None))

        # 2. [P1] Concurrency stress test (atomicity)
        db, _ = setup(root, 'concurrent', cpus=100, gpus=1)
        ids = [submit(db, cpus=1, mem=1, gpus=1) for _ in range(12)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            reservations = list(pool.map(db.try_allocate_job, ids))
        winners = sum(x is not None for x in reservations)
        p1_conc_pass = (winners == 1)
        assert p1_conc_pass, f"Expected 1 winner, got {winners}"
        emit("P1_concurrent_gpu_reservation", p1_conc_pass, winners=winners, contenders=len(ids))

        # 3. [P2] Node disable / removal updates to DRAIN
        db, cfg = setup(root, 'disabled')
        cfg['test']['enabled'] = False
        db.sync_nodes(cfg)
        nodes = db.list_nodes()
        is_drain = (nodes[0]['status'] == 'DRAIN')
        allocation = db.try_allocate_job(submit(db))
        p2_drain_pass = (is_drain and allocation is None)
        assert p2_drain_pass, f"Expected node DRAIN and no allocation, got {nodes[0]['status']} and {allocation}"
        emit("P2_disabled_node_drain", p2_drain_pass, node_state=nodes[0]['status'], blocked_allocation=(allocation is None))

        # 4. [P1] STARTING cancel safety (CANCEL_REQUESTED, keeps resources reserved until confirmed exit)
        db, _ = setup(root, 'cancel-starting', gpus=1)
        jid = submit(db, gpus=1)
        db.try_allocate_job(jid)
        cancel = load(BASE / 'bin/scancel')
        cancel.MiniSlurmDB = lambda path: db
        written_files = {}
        def fake_open(filepath, mode='r'):
            f = io.StringIO()
            written_files[filepath] = f
            return f
        with patch.object(sys, 'argv', ['scancel', str(jid)]), \
             patch('os.makedirs', return_value=None), \
             patch('builtins.open', side_effect=fake_open), \
             patch.object(subprocess, 'run', returncode=0), \
             contextlib.redirect_stdout(io.StringIO()):
            cancel.main()
        job_state = db.get_job(jid)['status']
        busy_gpus = db.list_nodes()[0]['busy_gpus']
        expected_cancel_file = f"/mnt/share/mini-slurm/jobs/{jid}/.cancel"
        cancel_marker_written = expected_cancel_file in written_files
        p1_cancel_pass = (job_state == 'CANCEL_REQUESTED' and (busy_gpus == [0] or busy_gpus == '0') and cancel_marker_written)
        assert p1_cancel_pass, f"Expected CANCEL_REQUESTED with GPU held and .cancel written, got {job_state}, busy_gpus={busy_gpus}, cancel_marker={cancel_marker_written}"
        emit("P1_starting_cancel_safety", p1_cancel_pass, state=job_state, gpus_held=busy_gpus)

        # 5. [P1] Negative resource input rejection & DB CHECK constraints
        try:
            parser.parse_sbatch_script('#SBATCH --cpus-per-task=-8\nexit 0', is_content=True)
            neg_rejected = False
        except ValueError:
            neg_rejected = True
        try:
            parser.parse_sbatch_script('#SBATCH --gres=gpu:-1\nexit 0', is_content=True)
            neg_gpu_rejected = False
        except ValueError:
            neg_gpu_rejected = True
        db, _ = setup(root, 'db-constraints')
        db_check_failed = False
        try:
            submit(db, cpus=-4)
        except Exception:
            db_check_failed = True
        p1_neg_pass = (neg_rejected and neg_gpu_rejected and db_check_failed)
        assert p1_neg_pass, f"Expected all negative checks to fail, got ({neg_rejected}, {neg_gpu_rejected}, {db_check_failed})"
        emit("P1_negative_resource_rejection", p1_neg_pass, neg_cpu_rejected=neg_rejected, neg_gpu_rejected=neg_gpu_rejected, db_check_enforced=db_check_failed)

        # 6. [P2] Space-separated directives parsing
        short, _ = parser.parse_sbatch_script(
            '#SBATCH -c 8\n#SBATCH --mem 16G\n#SBATCH --gres gpu:2\nexit 0', is_content=True)
        p2_space_pass = (short['cpus_per_task'] == 8 and short['mem_mb'] == 16384 and short['gpus'] == 2)
        assert p2_space_pass, f"Expected (8, 16384, 2), got {short}"
        emit("P2_space_separated_directives", p2_space_pass, cpus=short['cpus_per_task'], mem_mb=short['mem_mb'], gpus=short['gpus'])

        # 7. [P2] Time parsing strictness
        time_1_02 = parser.parse_time_seconds('1-02')  # 1 day 2 hours = 93600
        time_invalid = False
        try:
            parser.parse_time_seconds('1:2:3:4')
        except ValueError:
            time_invalid = True
        p2_time_pass = (time_1_02 == 93600 and time_invalid)
        assert p2_time_pass, f"Expected 93600 and ValueError, got {time_1_02} and {time_invalid}"
        emit("P2_time_format_strictness", p2_time_pass, day_hour_sec=time_1_02, invalid_rejected=time_invalid)

        # 8. [P1] SSH 255 does not kill live jobs prematurely
        job = dict(job_id=42, status='RUNNING', allocated_node='test', submit_time=0, dispatch_time=900)
        finished = scheduler_case(job, dict(status='RUNNING', pid=12345, last_heartbeat=1), 255, now=1000)
        p1_ssh_pass = (len(finished) == 0)
        assert p1_ssh_pass, f"Expected 0 finish calls on SSH 255, got {finished}"
        emit("P1_ssh_transport_error_handling", p1_ssh_pass, finish_calls=len(finished))

        # 9. [P1] Startup timeout uses dispatch_time, not submit_time
        job = dict(job_id=43, status='STARTING', allocated_node='test', submit_time=0, dispatch_time=980)
        # Dispatched 20s ago, submit_time 1000s ago. 20s < 45s timeout!
        finished = scheduler_case(job, None, 0, now=1000)
        p1_disp_pass = (len(finished) == 0)
        assert p1_disp_pass, f"Expected job not to timeout when dispatch_time is fresh, got {finished}"
        emit("P1_startup_timeout_uses_dispatch_time", p1_disp_pass, finish_calls=len(finished))

        # 10. [P1] Executor process group cleanup (no orphan children)
        testdir = root / 'executor'
        testdir.mkdir()
        script = testdir / 'job.sh'
        script.write_text('sleep 30 &\necho $! > child.pid\nsleep 0.2\nexit 0\n')
        cmd = [sys.executable, '-B', str(EXECUTOR), '--job-id', '999999999',
               '--script', str(script), '--chdir', str(testdir),
               '--stdout', str(testdir / 'stdout'), '--stderr', str(testdir / 'stderr'),
               '--gpus', '', '--cpus', '1', '--timeout', '5']
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        status = json.loads((testdir / '.status.json').read_text())
        child_pid = int((testdir / 'child.pid').read_text())
        child_stat = Path('/proc') / str(child_pid) / 'stat'
        alive = child_stat.exists() and child_stat.read_text().split(') ', 1)[1][0] != 'Z'
        p1_child_pass = (completed.returncode == 0 and status['status'] == 'COMPLETED' and not alive)
        assert p1_child_pass, f"Expected child to be dead, alive={alive}"
        emit("P1_executor_orphaned_process_cleanup", p1_child_pass, child_alive=alive, status=status['status'])

        # 11. [P2] GPU model mapping (a800 node -> a100 model)
        nodes_cfg = json.loads((BASE / 'etc/nodes.json').read_text())
        nodes_dict = nodes_cfg.get('nodes', nodes_cfg)
        a800_gpus = nodes_dict.get('a800', {}).get('gpus', [])
        models = {g.get('model') for g in a800_gpus}
        aliases = a800_gpus[0].get('aliases', []) if a800_gpus else []
        p2_gpu_pass = ('a100' in models and 'a800' in aliases)
        assert p2_gpu_pass, f"Expected a100 model and a800 alias, got models={models}, aliases={aliases}"
        emit("P2_gpu_hardware_model_mapping", p2_gpu_pass, models=list(models), aliases=aliases)

    print("=== ALL 11 VERIFICATION TESTS PASSED SUCCESSFULLY! ===", flush=True)

if __name__ == '__main__':
    run_all_tests()
