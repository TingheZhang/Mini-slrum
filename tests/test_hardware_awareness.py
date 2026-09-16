#!/usr/bin/env python3
import os
import sys
import tempfile
import json
import io
import contextlib
from pathlib import Path
import subprocess

BASE = Path(__file__).resolve().parent
if not (BASE / "lib").exists():
    BASE = BASE.parent
sys.path.insert(0, str(BASE / "lib"))

from mslurm_db import MiniSlurmDB

def test_hardware_awareness():
    print("=== Running Hardware Awareness & Physical Sensing Tests ===", flush=True)

    with tempfile.TemporaryDirectory(prefix="mslurm-hw-test-") as td:
        db_path = os.path.join(td, "test.db")
        db = MiniSlurmDB(db_path)

        # Register a test node with 2x RTX 6000 GPUs
        cfg = {
            "node1": {
                "ssh_target": "node1",
                "python_bin": "python3",
                "cpus": 16,
                "mem_mb": 64000,
                "gpus": [
                    {"id": 0, "model": "rtx6000", "aliases": ["rtx6000"]},
                    {"id": 1, "model": "rtx6000", "aliases": ["rtx6000"]}
                ],
                "enabled": True
            }
        }
        db.sync_nodes(cfg)

        # 1. Initially both GPUs are clean and idle
        nodes = db.list_nodes()
        assert len(nodes) == 1
        n = nodes[0]
        assert n["idle_gpus"] == [0, 1]
        assert n["busy_gpus"] == []
        assert n["ext_busy_gpus"] == []
        print("[PASS] Initial state: both GPUs detected as idle", flush=True)

        # 2. Simulate physical telemetry: GPU 0 is externally busy (48GB used by another user), GPU 1 is idle (15MB)
        telemetry = {
            "0": {"used_mb": 48000, "total_mb": 97887, "util": 85, "ext_busy": True},
            "1": {"used_mb": 15, "total_mb": 97887, "util": 0, "ext_busy": False}
        }
        db.update_node_telemetry("node1", telemetry)

        nodes = db.list_nodes()
        n = nodes[0]
        assert n["ext_busy_gpus"] == [0], f"Expected ext_busy_gpus=[0], got {n['ext_busy_gpus']}"
        assert n["idle_gpus"] == [1], f"Expected idle_gpus=[1], got {n['idle_gpus']}"
        assert n["busy_gpus"] == []
        print("[PASS] Physical telemetry sensing: GPU 0 correctly tagged as ext_busy, GPU 1 as idle", flush=True)

        # 3. Submit a job requiring 1 GPU: scheduler MUST avoid GPU 0 and pick GPU 1!
        jid1 = db.submit_job(
            name="job1", user="user1", req_cpus=2, req_mem_mb=4000, req_gpus=1,
            gres_model="rtx6000", time_limit_sec=60, chdir="/tmp", script_content="exit 0",
            stdout_path=None, stderr_path=None
        )
        alloc1 = db.try_allocate_job(jid1)
        assert alloc1 is not None, "Job 1 should be allocated to GPU 1"
        assert alloc1["allocated_gpus"] == [1], f"Expected allocation on GPU 1 (avoiding GPU 0), got {alloc1['allocated_gpus']}"
        print("[PASS] Collision avoidance: Job 1 successfully avoided externally busy GPU 0 and allocated GPU 1", flush=True)

        # 4. Submit Job 2 requiring 1 GPU: since GPU 0 is EXT_BUSY and GPU 1 is SLURM_BUSY, Job 2 MUST wait in PENDING!
        jid2 = db.submit_job(
            name="job2", user="user1", req_cpus=2, req_mem_mb=4000, req_gpus=1,
            gres_model="rtx6000", time_limit_sec=60, chdir="/tmp", script_content="exit 0",
            stdout_path=None, stderr_path=None
        )
        alloc2 = db.try_allocate_job(jid2)
        assert alloc2 is None, f"Expected Job 2 to stay PENDING, but got allocation: {alloc2}"
        print("[PASS] Anti-OOM protection: Job 2 safely stayed PENDING instead of colliding with externally busy GPU 0", flush=True)

        # 5. Simulate external user finishing on GPU 0 (VRAM drops to 15MB)
        telemetry_clean = {
            "0": {"used_mb": 15, "total_mb": 97887, "util": 0, "ext_busy": False},
            "1": {"used_mb": 1000, "total_mb": 97887, "util": 20, "ext_busy": False, "slurm_job_id": jid1}
        }
        db.update_node_telemetry("node1", telemetry_clean)

        # Now Job 2 can safely be allocated to GPU 0!
        alloc2_retry = db.try_allocate_job(jid2)
        assert alloc2_retry is not None, "Job 2 should be allocated now that GPU 0 is free"
        assert alloc2_retry["allocated_gpus"] == [0], f"Expected GPU 0, got {alloc2_retry['allocated_gpus']}"
        print("[PASS] Dynamic recovery: Job 2 successfully allocated to GPU 0 after external release", flush=True)

        # 6. Finish jobs and verify clean release
        db.finish_job(jid1, "COMPLETED", 0)
        db.finish_job(jid2, "COMPLETED", 0)
        nodes_final = db.list_nodes()
        assert nodes_final[0]["busy_gpus"] == []
        print("[PASS] Job completion and resource reclamation verified", flush=True)

        # 7. Verify sinfo and sinfo -l CLI commands with telemetry
        telemetry_display = {
            "0": {"used_mb": 48120, "total_mb": 97887, "util": 78, "ext_busy": True},
            "1": {"used_mb": 15, "total_mb": 97887, "util": 0, "ext_busy": False}
        }
        db.update_node_telemetry("node1", telemetry_display)

        # Run sinfo (summary)
        env = dict(os.environ, MSLURM_DB=db_path)
        p_summary = subprocess.run([sys.executable, str(BASE / "bin/sinfo")], env=env, capture_output=True, text=True)
        assert p_summary.returncode == 0
        assert "0/1/1/2" in p_summary.stdout, f"Expected 0/1/1/2 in sinfo output: {p_summary.stdout}"
        print("[PASS] sinfo summary output correctly displays GPUS(A/E/I/T) as 0/1/1/2", flush=True)

        # Run sinfo -l (detail)
        p_long = subprocess.run([sys.executable, str(BASE / "bin/sinfo"), "-l"], env=env, capture_output=True, text=True)
        assert p_long.returncode == 0
        assert "EXT_BUSY" in p_long.stdout, f"Expected EXT_BUSY in sinfo -l: {p_long.stdout}"
        assert "IDLE" in p_long.stdout, f"Expected IDLE in sinfo -l: {p_long.stdout}"
        assert "47.0 GiB" in p_long.stdout or "48120" in p_long.stdout, f"Expected VRAM in sinfo -l: {p_long.stdout}"
        print("[PASS] sinfo -l detailed output correctly displays EXT_BUSY and VRAM usage", flush=True)

        # Force garbage collection on Windows to release SQLite handles
        del db
        import gc
        gc.collect()

    print("=== ALL HARDWARE AWARENESS TESTS PASSED SUCCESSFULLY! ===", flush=True)

if __name__ == "__main__":
    test_hardware_awareness()
