import sys
import time
import runpy
from pathlib import Path

BASE = Path('/home/lisy/mini-slurm')
sys.path.insert(0, str(BASE / 'lib'))
sched = runpy.run_path(str(BASE / 'sbin/mslurm-sched'), run_name='audit3')
probe = sched['probe_remote_liveness']

ret1 = probe('a800', 918273645, sid=None)
ret2 = probe('a800', 918273645, sid=2147483647)
print(f"probe(a800, 918273645, sid=None) -> {ret1}")
print(f"probe(a800, 918273645, sid=2147483647) -> {ret2}")
assert ret1 == 1, f"Expected 1, got {ret1}"
assert ret2 == 1, f"Expected 1, got {ret2}"

# Test T2: Prefix boundary check on a800
import subprocess
dummy = subprocess.Popen(["ssh", "a800", "/opt/conda/envs/stack-share/bin/python", "-c", "'import time; time.sleep(30)'",
                          "mslurm-executor", "--job-id", "9182736450"])
try:
    time.sleep(1)
    ret_prefix = probe('a800', 918273645)
    ret_exact = probe('a800', 9182736450)
    print(f"probe(a800, 918273645) against dummy 9182736450 -> {ret_prefix}")
    print(f"probe(a800, 9182736450) against dummy 9182736450 -> {ret_exact}")
    assert ret_prefix == 1, f"Expected 1 (not matched), got {ret_prefix}"
    assert ret_exact == 0, f"Expected 0 (exact match), got {ret_exact}"
    print("PREFIX TESTS PASSED: Prefix matching does NOT match other jobs!")
finally:
    dummy.terminate()
    dummy.wait()

print("PROBE TESTS PASSED: Nonexistent jobs accurately return 1 (DEAD)!")
