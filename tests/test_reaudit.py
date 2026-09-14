"""Comprehensive verification script testing all 8 items from MINI_SLURM_REAUDIT_2026-09-14.md.

Run on 174: python3 -B verify_reaudit_safety.py
"""
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

BASE = Path('/home/lisy/mini-slurm')
EXEC = Path('/mnt/share/mini-slurm/libexec/mslurm-executor')

def load(path):
    m = types.ModuleType('verified_' + path.name.replace('.', '_'))
    m.__file__ = str(path)
    exec(compile(path.read_text(), str(path), 'exec'), m.__dict__)
    return m

def emit(label, passed, **kwargs):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {label}: {json.dumps(kwargs, ensure_ascii=False)}", flush=True)

class EndLoop(BaseException):
    pass

def tick(db, job, status, ssh_code=0, dispatch=False, probe_code=1):
    """Runs one scheduler tick with mocked I/O and captures calls."""
    os.environ["MSLURM_NO_LOCK"] = "1"
    sched = load(BASE / 'sbin/mslurm-sched')
    reads = []
    ssh_calls = []
    writes = []
    sched.MiniSlurmDB = lambda path: types.SimpleNamespace(
        sync_nodes=lambda cfg: None,
        list_jobs=lambda **kw: [] if dispatch else [db.get_job(job)],
        get_pending_jobs=lambda: [db.get_job(job)] if dispatch else [],
        try_allocate_job=db.try_allocate_job,
        get_job=db.get_job,
        finish_job=db.finish_job,
        mark_job_running=db.mark_job_running,
        backup=lambda path: None,
    )
    sched.load_nodes_config = lambda path: {'test': {'ssh_target': 'fake-target'}}
    sched.time = types.SimpleNamespace(time=lambda: 1000,
        sleep=lambda delay: (_ for _ in ()).throw(EndLoop()))
    sched.os = types.SimpleNamespace(path=types.SimpleNamespace(join=os.path.join,
        exists=lambda path: status is not None if path.endswith('.status.json') else True),
        makedirs=lambda *a, **kw: None, chmod=lambda *a: None)

    def fake_open(path, mode='r', **kwargs):
        if 'w' in mode:
            writes.append(path)
            return io.StringIO()
        reads.append(path)
        return io.StringIO(json.dumps(status))

    def fake_run(args, **kwargs):
        ssh_calls.append(args)
        # If args is a probe or kill command, return probe_code
        cmd_str = " ".join(args) if isinstance(args, list) else str(args)
        if "python3 -c" in cmd_str or "pgrep" in cmd_str or "pkill" in cmd_str:
            return types.SimpleNamespace(returncode=probe_code)
        return types.SimpleNamespace(returncode=ssh_code)

    sched.open = fake_open
    sched.subprocess = types.SimpleNamespace(run=fake_run, DEVNULL=None)
    with contextlib.redirect_stdout(io.StringIO()):
        try:
            sched.main()
        except EndLoop:
            pass
    return dict(state=db.get_job(job)['status'],
                held_gpus=db.list_nodes()[0]['busy_gpus'],
                ssh_calls=len(ssh_calls), writes=writes, calls=ssh_calls)

def setup(root, name):
    dbmod = load(BASE / 'lib/mslurm_db.py')
    db = dbmod.MiniSlurmDB(str(root / name / 'audit.db'))
    db.sync_nodes({'test': {'cpus': 4, 'mem_mb': 4000,
                            'gpus': [{'id': 0, 'model': 'a100', 'aliases': ['a800']}]}})
    return db

def submit(db, model=None):
    return db.submit_job('reaudit', 'audit', 1, 1000, 1, model,
                         10, '/tmp', 'exit 0', None, None)

def allocate_old(db, jid):
    db.try_allocate_job(jid)
    with db.get_conn() as con:
        con.execute('UPDATE jobs SET dispatch_time=900, submit_time=100 WHERE job_id=?', (jid,))

def alive(pid):
    p = Path('/proc') / str(pid) / 'stat'
    return p.exists() and p.read_text().split(') ', 1)[1][0] != 'Z'

def exec_case(root, name, script_text, pre_cancel=False):
    work = root / name
    work.mkdir()
    script = work / 'job.sh'
    script.write_text(script_text)
    if pre_cancel:
        (work / '.cancel').write_text('CANCEL')
    args = [sys.executable, '-B', str(EXEC), '--job-id', '999999998',
            '--script', str(script), '--chdir', str(work), '--stdout', str(work / 'out'),
            '--stderr', str(work / 'err'), '--gpus', '', '--cpus', '1', '--timeout', '10']
    child = None
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=15)
        data = json.loads((work / '.status.json').read_text())
        result = dict(exit=proc.returncode, status=data['status'],
                      script_side_effect=(work / 'ran').exists())
        if (work / 'child.pid').exists():
            child = int((work / 'child.pid').read_text())
            is_alive = alive(child)
            child_pgid = None
            child_sid = None
            if is_alive:
                try:
                    child_pgid = os.getpgid(child)
                    child_sid = os.getsid(child)
                except ProcessLookupError:
                    is_alive = False
            result.update(child_alive=is_alive, child_pgid=child_pgid,
                          child_sid=child_sid, job_pgid=data.get('pgid'))
        return result
    finally:
        if child is not None and alive(child):
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass

def run_tests():
    print("=== Running Comprehensive Re-Audit Safety Verification on 174 ===", flush=True)
    dbmod = load(BASE / 'lib/mslurm_db.py')
    parser = load(BASE / 'lib/mslurm_parser.py')

    with tempfile.TemporaryDirectory(prefix='mslurm-test-') as td:
        root = Path(td)

        # 1. [R1] Launch SSH 255: does NOT release GPU or fail immediately without confirmation
        db = setup(root, 'launch-unknown')
        jid = submit(db)
        # Dispatch with SSH code 255 and probe code 255 (host unreachable)
        result = tick(db, jid, None, ssh_code=255, dispatch=True, probe_code=255)
        r1_launch_pass = (result['held_gpus'] == [0] and result['state'] == 'STARTING')
        assert r1_launch_pass, f"Expected held_gpus=[0] and STARTING on SSH 255, got {result}"
        emit("R1_launch_ssh_255_preserves_reservation", r1_launch_pass, **result)

        # 2. [R1] Startup timeout: makes SSH probe before deciding
        db = setup(root, 'start-timeout-probe')
        jid = submit(db)
        allocate_old(db, jid)
        # Probe returns 1 (confirmed no process running on node)
        result = tick(db, jid, None, ssh_code=0, probe_code=1)
        r1_probe_pass = (result['state'] == 'FAILED' and result['ssh_calls'] > 0 and result['held_gpus'] == [])
        assert r1_probe_pass, f"Expected FAILED with probe ssh_calls > 0, got {result}"
        emit("R1_startup_timeout_probes_before_release", r1_probe_pass, **result)

        # 3. [R1] Startup timeout when probe returns 0 (process IS running): keeps reservation
        db = setup(root, 'start-timeout-alive')
        jid = submit(db)
        allocate_old(db, jid)
        result = tick(db, jid, None, ssh_code=0, probe_code=0)
        r1_alive_pass = (result['state'] == 'STARTING' and result['held_gpus'] == [0])
        assert r1_alive_pass, f"Expected STARTING and held_gpus=[0] when remote process is alive, got {result}"
        emit("R1_startup_timeout_preserves_running_process", r1_alive_pass, **result)

        # 4. [R5] CANCEL_REQUESTED without status file reconciles via probe
        db = setup(root, 'cancel-no-executor')
        jid = submit(db)
        allocate_old(db, jid)
        db.request_cancel(jid)
        # Probe confirms no process is running (probe_code=1)
        result = tick(db, jid, None, probe_code=1)
        r5_pass = (result['state'] == 'CANCELLED' and result['held_gpus'] == [] and result['ssh_calls'] > 0)
        assert r5_pass, f"Expected CANCELLED with ssh_calls > 0, got {result}"
        emit("R5_cancel_without_executor_reconciled", r5_pass, **result)

        # 5. [R3] Pre-existing .cancel: script NEVER executes side-effects
        res = exec_case(root, 'pre-cancel', 'echo ran > ran\nsleep 0.2\n', pre_cancel=True)
        r3_pass = (res['status'] == 'CANCELLED' and res['script_side_effect'] is False)
        assert r3_pass, f"Expected CANCELLED with no side effects, got {res}"
        emit("R3_precancel_no_script_execution", r3_pass, **res)

        # 6. [R4] set -m background process in new PGID is killed along with session
        res = exec_case(root, 'same-session-new-group',
                        'set -m\nsleep 30 &\necho $! > child.pid\nsleep 0.2\nexit 0\n')
        r4_pass = (res['status'] == 'COMPLETED' and res.get('child_alive') is False)
        assert r4_pass, f"Expected child_alive=False, got {res}"
        emit("R4_session_cleanup_kills_job_control_children", r4_pass, **res)

        # 7. [R7] Directive order invariance: -C vs --gres
        first, _ = parser.parse_sbatch_script('#SBATCH -C rtx6000\n#SBATCH --gres gpu:1\nexit 0', is_content=True)
        last, _ = parser.parse_sbatch_script('#SBATCH --gres gpu:1\n#SBATCH -C rtx6000\nexit 0', is_content=True)
        r7_order_pass = (first['gres_model'] == 'rtx6000' and last['gres_model'] == 'rtx6000')
        assert r7_order_pass, f"Expected both rtx6000, got first={first['gres_model']}, last={last['gres_model']}"
        emit("R7_constraint_order_invariance", r7_order_pass, first=first['gres_model'], last=last['gres_model'])

        # 8. [R7] Exact GPU model matching: a1000 does NOT match a100
        db = setup(root, 'gpu-model-exact')
        allocation = db.try_allocate_job(submit(db, 'a1000'))
        r7_match_pass = (allocation is None)
        assert r7_match_pass, f"Expected None for a1000, got {allocation}"
        emit("R7_exact_gpu_model_matching_rejects_a1000", r7_match_pass, allocation=allocation)

        # 9. [R6] Schema migration adds CHECK constraints to existing tables & blocks negative values
        ddls = list(db.get_conn().execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name IN ('jobs','nodes','gpu_allocations','cpu_mem_allocations')"))
        checks = {name: 'CHECK' in ddl.upper() for name, ddl in ddls}
        r6_schema_pass = (checks['jobs'] is True and checks['cpu_mem_allocations'] is True)
        assert r6_schema_pass, f"Expected CHECK in jobs and cpu_mem_allocations, got {checks}"

        # Test direct SQL rejection
        sql_check_passed = False
        try:
            with db.get_conn() as con:
                con.execute('INSERT INTO jobs (req_cpus,req_mem_mb,req_gpus) VALUES (-8,1000,0)')
        except sqlite3.IntegrityError:
            sql_check_passed = True
        assert sql_check_passed, "Direct SQL insert of negative cpus must fail with IntegrityError"
        emit("R6_database_schema_check_constraints_enforced", r6_schema_pass and sql_check_passed,
             schema_checks=checks, direct_sql_rejected=sql_check_passed)

        # 10. [R8] Daemon flock prevents concurrent start
        lock_file = '/home/lisy/mini-slurm/var/mslurm-sched.lock'
        import fcntl
        with open(lock_file, 'r') as lf:
            try:
                fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = False
                fcntl.flock(lf, fcntl.LOCK_UN)
            except (BlockingIOError, IOError):
                locked = True
        assert locked, "mslurm-sched.lock must be locked by running daemon PID"
        emit("R8_daemon_single_instance_flock_active", locked)

    print("=== ALL 10 RE-AUDIT VERIFICATION TESTS PASSED SUCCESSFULLY! ===", flush=True)

if __name__ == '__main__':
    run_tests()
