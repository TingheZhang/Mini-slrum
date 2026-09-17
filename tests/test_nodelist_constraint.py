import unittest
import os
import sys
import tempfile
import json
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.exists(os.path.join(BASE_DIR, "lib")):
    BASE_DIR = r"C:\Users\tiz125\.gemini\antigravity\scratch\mini-slurm"
sys.path.insert(0, os.path.join(BASE_DIR, "lib"))

from mslurm_parser import parse_sbatch_script
from mslurm_db import MiniSlurmDB

class TestNodelistConstraint(unittest.TestCase):
    def test_parser_nodelist_and_constraint(self):
        # 1. -w flag with space
        script1 = "#!/bin/bash\n#SBATCH -w 176\n#SBATCH -c 4\necho hi\n"
        params, _ = parse_sbatch_script(script1, is_content=True)
        self.assertEqual(params["nodelist"], "176")
        self.assertEqual(params["cpus_per_task"], 4)

        # 2. --nodelist=flag
        script2 = "#!/bin/bash\n#SBATCH --nodelist=176,174\n#SBATCH -C rtx2080ti\necho hi\n"
        params, _ = parse_sbatch_script(script2, is_content=True)
        self.assertEqual(params["nodelist"], "176,174")
        self.assertEqual(params["gres_model"], "rtx2080ti")

        # 3. Compact short options -w176 and -C2080ti
        script3 = "#!/bin/bash\n#SBATCH -w176\n#SBATCH -C2080ti\necho hi\n"
        params, _ = parse_sbatch_script(script3, is_content=True)
        self.assertEqual(params["nodelist"], "176")
        self.assertEqual(params["gres_model"], "2080ti")

    def test_scheduling_with_nodelist_and_constraint(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = MiniSlurmDB(db_path)

            nodes_cfg = {
                "172share": {
                    "ssh_target": "172share",
                    "python_bin": "python3",
                    "cpus": 24,
                    "mem_mb": 120000,
                    "gpus": [{"id": 0, "model": "rtx6000", "aliases": ["rtx6000"]}],
                    "enabled": True
                },
                "174": {
                    "ssh_target": "localhost",
                    "python_bin": "python3",
                    "cpus": 36,
                    "mem_mb": 120000,
                    "gpus": [],
                    "enabled": True
                },
                "176": {
                    "ssh_target": "176",
                    "python_bin": "python3",
                    "cpus": 24,
                    "mem_mb": 120000,
                    "gpus": [{"id": 0, "model": "rtx2080ti", "aliases": ["rtx2080ti", "2080ti"]}],
                    "enabled": True
                },
                "a800": {
                    "ssh_target": "a800",
                    "python_bin": "python3",
                    "cpus": 24,
                    "mem_mb": 120000,
                    "gpus": [{"id": 0, "model": "a100", "aliases": ["a800", "a100"]}],
                    "enabled": True
                }
            }
            db.sync_nodes(nodes_cfg)

            # Test 1: Pure CPU job with -w 176 (Pin to 176)
            j1 = db.submit_job(
                name="cpu_pin_176", user="test", req_cpus=4, req_mem_mb=8000,
                req_gpus=0, gres_model=None, time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 1", stdout_path=None, stderr_path=None,
                req_nodelist="176"
            )
            alloc1 = db.try_allocate_job(j1)
            self.assertIsNotNone(alloc1)
            self.assertEqual(alloc1["node_name"], "176")
            self.assertEqual(alloc1["allocated_cpus"], 4)
            self.assertEqual(alloc1["allocated_gpus"], [])
            db.finish_job(j1, "COMPLETED", 0)

            # Test 2: Pure CPU job with -C rtx2080ti (Must land on 176, NOT 172share!)
            j2 = db.submit_job(
                name="cpu_constraint_2080ti", user="test", req_cpus=4, req_mem_mb=8000,
                req_gpus=0, gres_model="rtx2080ti", time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 2", stdout_path=None, stderr_path=None
            )
            alloc2 = db.try_allocate_job(j2)
            self.assertIsNotNone(alloc2)
            self.assertEqual(alloc2["node_name"], "176")
            self.assertEqual(alloc2["allocated_gpus"], [])
            db.finish_job(j2, "COMPLETED", 0)

            # Test 3: Pure CPU job with -C a100 (Must land on a800)
            j3 = db.submit_job(
                name="cpu_constraint_a100", user="test", req_cpus=4, req_mem_mb=8000,
                req_gpus=0, gres_model="a100", time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 3", stdout_path=None, stderr_path=None
            )
            alloc3 = db.try_allocate_job(j3)
            self.assertIsNotNone(alloc3)
            self.assertEqual(alloc3["node_name"], "a800")
            db.finish_job(j3, "COMPLETED", 0)

            # Test 4: Pure CPU job with -w 174 (Must land on 174)
            j4 = db.submit_job(
                name="cpu_pin_174", user="test", req_cpus=8, req_mem_mb=16000,
                req_gpus=0, gres_model=None, time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 4", stdout_path=None, stderr_path=None,
                req_nodelist="174"
            )
            alloc4 = db.try_allocate_job(j4)
            self.assertIsNotNone(alloc4)
            self.assertEqual(alloc4["node_name"], "174")
            db.finish_job(j4, "COMPLETED", 0)

            # Test 5: GPU job with -w 176 --gres=gpu:1
            j5 = db.submit_job(
                name="gpu_pin_176", user="test", req_cpus=4, req_mem_mb=8000,
                req_gpus=1, gres_model=None, time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 5", stdout_path=None, stderr_path=None,
                req_nodelist="176"
            )
            alloc5 = db.try_allocate_job(j5)
            self.assertIsNotNone(alloc5)
            self.assertEqual(alloc5["node_name"], "176")
            self.assertEqual(alloc5["allocated_gpus"], [0])
            db.finish_job(j5, "COMPLETED", 0)

            # Test 6: Mismatched nodelist and constraint (-w 172share -C rtx2080ti)
            j6 = db.submit_job(
                name="mismatch", user="test", req_cpus=4, req_mem_mb=8000,
                req_gpus=0, gres_model="rtx2080ti", time_limit_sec=60, chdir="/mnt/share",
                script_content="echo 6", stdout_path=None, stderr_path=None,
                req_nodelist="172share"
            )
            alloc6 = db.try_allocate_job(j6)
            self.assertIsNone(alloc6) # Cannot allocate!

            # Test 7: Nonexistent node (-w fake_node) must raise ValueError on submit
            with self.assertRaises(ValueError) as ctx:
                db.submit_job(
                    name="fake_node", user="test", req_cpus=4, req_mem_mb=8000,
                    req_gpus=0, gres_model=None, time_limit_sec=60, chdir="/mnt/share",
                    script_content="echo 7", stdout_path=None, stderr_path=None,
                    req_nodelist="fake_node"
                )
            self.assertIn("Invalid node 'fake_node'", str(ctx.exception))

            # Test 8: Empty nodelist must raise ValueError
            with self.assertRaises(ValueError):
                db.submit_job(
                    name="empty_node", user="test", req_cpus=4, req_mem_mb=8000,
                    req_gpus=0, gres_model=None, time_limit_sec=60, chdir="/mnt/share",
                    script_content="echo 8", stdout_path=None, stderr_path=None,
                    req_nodelist=""
                )

if __name__ == "__main__":
    unittest.main()
