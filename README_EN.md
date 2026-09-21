# Mini-Slurm 🚀

<p align="center">
  <strong>Lightweight, Rootless, Daemonless Distributed CPU/GPU Job Scheduler</strong><br>
  <em>Compatible with Slurm semantics, featuring real-time physical GPU telemetry and native AI Agent skills.</em>
</p>

<p align="center">
  <a href="README_EN.md">🇬🇧 English</a> | <a href="README.md">🇨🇳 简体中文</a>
</p>

<p align="center">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.8%2B-blue.svg" alt="Python: 3.8+"></a>
  <a href="#-key-features"><img src="https://img.shields.io/badge/Dependencies-Standard%20Library%20Only-brightgreen.svg" alt="Zero Dependencies"></a>
  <a href="#-job-submission--management"><img src="https://img.shields.io/badge/CLI-Slurm%20Compatible-orange.svg" alt="Slurm Compatible"></a>
  <a href="#-automated-testing--safety-verification"><img src="https://img.shields.io/badge/Tests-Full%20Audit%20Passed-success.svg" alt="Battle Tested"></a>
</p>

---

**Mini-Slurm** is a lightweight, rootless, daemonless distributed job scheduler designed specifically for small-to-medium multi-node CPU/GPU computing clusters.

Built entirely on the **Python standard library + SQLite WAL + OpenSSH + Shared Filesystem (NFS/Ceph/SMB)**, Mini-Slurm delivers the authentic Slurm user experience (`sbatch`, `squeue`, `sinfo`, `scancel`, `sacct`) without requiring resident worker daemons, root privileges, or third-party packages. It also natively embeds an **Agent Skill** tailored for LLM coding assistants (Antigravity, Claude Code, Codex, etc.).

---

## 🌟 Key Features

- **Zero Third-Party Dependencies**: Pure Python 3.8+ standard library only (`sqlite3`, `subprocess`, `fcntl`, `shlex`, `json`, etc.). Zero `pip install` required.
- **Zero Resident Daemons on Compute Nodes**: Workers require no `slurmd`, Redis, or RPC daemons. The master scheduler spawns lightweight on-demand executors (`mslurm-executor`) over standard OpenSSH sessions that cleanly terminate upon job completion.
- **Slurm CLI Compatibility**:
  - `sbatch`: Supports standard `#SBATCH` directives (`-c`, `--mem`, `--gres=gpu:N`, `-C/--constraint`, `-w/--nodelist`, `--time`, `-o`, `-e`, etc.).
  - `squeue`: Real-time queue inspection showing job status, elapsed time, allocated nodes, and assigned GPU device IDs.
  - `sinfo`: Cluster overview with node health (UP/DOWN/DRAIN), CPU/RAM utilization, and real-time GPU allocation.
  - `scancel`: Signal-level job cancellation with recursive process tree reaping across remote sessions.
  - `sacct`: Historic accounting, exit codes, elapsed runtime, and audit trails.
  - `mslurm`: Daemon lifecycle management (`start`, `stop`, `restart`, `status`, `logs`).
- **Transparent Hardware Sensing & Anti-Collision**:
  - Built for shared research clusters! The scheduler concurrently probes remote physical GPUs via `nvidia-smi`.
  - External non-Slurm tasks (e.g. standalone training scripts or Docker containers) are automatically tagged as `EXT_BUSY` and avoided, preventing out-of-memory (OOM) collisions.
  - `sinfo` displays `GPUS(A/E/I/T)` — **Allocated by Slurm / Externally Busy / Clean Idle / Total**.
  - `sinfo -l` shows per-GPU physical telemetry, distinguishing exact VRAM usage and ownership.
  - **Fail-Closed Safety**: Suspends new allocations on nodes with missing, corrupted, or stale telemetry.
- **Hardened Safety & Concurrency Engineering**:
  - **Clock Skew Immunity**: Evaluates monotonically increasing sequence counters (`heartbeat_seq`) against local scheduler monotonic clocks, unaffected by hours of remote clock drift.
  - **Network Failure Protection**: Holds GPU reservations when SSH returns exit code 255 (network disconnect or node unreachable), only releasing resources after positive verification that processes are dead.
  - **Token-Level Process Probing**: Uses `/proc/*/cmdline` NUL-delimited token matching to eliminate false positive self-matching and job ID prefix collision (e.g. cancelling Job 1 will never affect Job 10 or 11).
  - **Recursive Session Reaping**: Pre-registers signal handlers before `Popen`; recursively traverses `/proc` process hierarchies to clean up child forks and `set -m` job-control trees, leaving zero orphaned processes.
  - **SQLite WAL & Integrity Constraints**: Atomic ledger with exclusive file locking, table-level `CHECK` constraints against negative resource requests, and automated database schema migration.
- **Native AI Agent Integration**: Includes `skills/mini-slurm/SKILL.md`, allowing AI coding assistants to autonomously inspect node availability, select hardware constraints, dispatch experiments, and monitor logs.

---

## 🏗️ Architecture

```mermaid
flowchart TD
    User["User / AI Agent (Antigravity / Claude)"]
    User -->|sbatch / squeue / scancel / sinfo / sacct| Sched

    subgraph MasterNode["Master Node (Head Node)"]
        Sched["mslurm-sched (Scheduler Daemon)"]
        DB[(SQLite WAL<br/>Ledger & Queue)]
        Lock["mslurm-sched.lock<br/>Exclusive flock"]
        Sched <--> DB
        Sched --- Lock
    end

    Sched -->|OpenSSH (On-demand, No Agent)| WorkerA["GPU Node A (e.g. 8x A100 80GB)"]
    Sched -->|OpenSSH (On-demand, No Agent)| WorkerB["GPU Node B (e.g. 4x RTX 6000)"]
    Sched -->|OpenSSH (On-demand, No Agent)| WorkerC["GPU Node C (e.g. 4x RTX 2080Ti)"]
    Sched -->|OpenSSH (On-demand, No Agent)| WorkerD["CPU Node D (General Compute)"]

    subgraph SharedStorage["Shared Cluster Storage (NFS / Ceph / SMB)"]
        Exec["libexec/mslurm-executor"]
        JobsDir["jobs/{job_id}/<br/>├── job.sh<br/>├── slurm.out / slurm.err<br/>├── .status.json<br/>└── .cancel"]
    end

    WorkerA <--> SharedStorage
    WorkerB <--> SharedStorage
    WorkerC <--> SharedStorage
    WorkerD <--> SharedStorage
```

---

## 🚀 Quick Start

### 1. Prerequisites

1. **Operating System**: Linux (Ubuntu 20.04/22.04+, Debian, CentOS, RHEL, or Rocky Linux).
2. **Python**: Python 3.8+ on both the head node and compute nodes.
3. **SSH Keys**: Passwordless SSH access from the head node to all compute nodes (`ssh <node_target>`).
4. **Shared Storage**: A shared filesystem (e.g. `/mnt/share`) mounted across all nodes.

### 2. Installation

Clone Mini-Slurm to your head node or shared filesystem:

```bash
git clone https://github.com/TingheZhang/Mini-slrum.git /mnt/share/mini-slurm
cd /mnt/share/mini-slurm
```

Add the binaries to your `PATH` (e.g. in `~/.bashrc`):
```bash
export PATH="/mnt/share/mini-slurm/bin:$PATH"
```

### 3. Configure Cluster Nodes (`etc/nodes.json`)

Copy the configuration template and customize your cluster topology:
```bash
cp etc/nodes.json.example etc/nodes.json
vim etc/nodes.json
```

Example configuration:
```json
{
  "nodes": {
    "gpu-node01": {
      "ssh_target": "node01",
      "python_bin": "/opt/conda/bin/python",
      "cpus": 80,
      "mem_mb": 160000,
      "gpus": [
        {"id": 0, "model": "a100", "aliases": ["a100", "a800", "a100-80gb"]},
        {"id": 1, "model": "a100", "aliases": ["a100", "a800", "a100-80gb"]}
      ],
      "enabled": true
    },
    "gpu-node02": {
      "ssh_target": "node02",
      "python_bin": "/usr/bin/python3",
      "cpus": 24,
      "mem_mb": 110000,
      "gpus": [
        {"id": 0, "model": "rtx6000", "aliases": ["rtx6000", "blackwell"]}
      ],
      "enabled": true
    },
    "cpu-node01": {
      "ssh_target": "node03",
      "python_bin": "/usr/bin/python3",
      "cpus": 36,
      "mem_mb": 120000,
      "gpus": [],
      "enabled": true
    }
  }
}
```

> [!TIP]
> If a compute node runs inside a **Docker Container**, configure `cpus` and `mem_mb` according to the container's actual cgroups quota (inspect `/sys/fs/cgroup/cpu.max` and `/sys/fs/cgroup/memory.max`) to prevent out-of-memory container kills by the host.

### 4. Start the Scheduler Daemon

```bash
# Start the scheduler
mslurm start

# Check service health
mslurm status

# Inspect cluster nodes
sinfo
```

Example output:
```text
NODELIST     STATE    CPUS(A/I/T)    MEM(A/T MB)        GPUS(A/E/I/T)    GPU_MODELS
gpu-node01   UP       0/80/80        0/160000           0/0/8/8          a100
gpu-node02   UP       0/24/24        0/110000           0/1/3/4          rtx6000
gpu-node03   UP       0/10/10        0/80000            0/0/4/4          rtx2080ti
cpu-node01   UP       0/36/36        0/120000           0/0/0/0          none
```

View real-time physical GPU telemetry and external occupancy:
```bash
sinfo -l
```

---

## 📝 Job Submission & Management

### 1. Submit a GPU Job (`run_gpu.sbatch`)

```bash
#!/bin/bash
#SBATCH --job-name=train_model
#SBATCH --cpus-per-task=4
#SBATCH --mem=16000
#SBATCH --gres=gpu:1
#SBATCH -C a100
#SBATCH --time=01:00:00
#SBATCH --output=/mnt/share/logs/%j.out
#SBATCH --error=/mnt/share/logs/%j.err

echo "Running on host: $(hostname)"
echo "Assigned GPU device: $CUDA_VISIBLE_DEVICES"
python3 train.py
```

Submit and track:
```bash
$ sbatch run_gpu.sbatch
Submitted batch job 1

$ squeue
JOBID    NAME             USER       ST         TIME       NODELIST(RESOURCES)     
1        train_model      alice      RUNNING    00:05      gpu-node01:gpu[0]
```

### 2. Submit a CPU-Only Job (with Node Pinning)

```bash
#!/bin/bash
#SBATCH --job-name=data_prep
#SBATCH --cpus-per-task=8
#SBATCH --mem=32000
#SBATCH -w cpu-node01
#SBATCH --time=00:30:00

python3 preprocess.py
```

### 3. Cancel a Job

```bash
$ scancel 1
Cancellation requested for job 1 on gpu-node01
```
Mini-Slurm transmits a cancel signal to the remote executor, recursively kills the entire process group within seconds, and atomically reclaims reserved GPUs and CPUs.

### 4. Query Accounting History

```bash
$ sacct
JobID    JobName          Node        State        ExitCode   Elapsed    SubmitTime          
1        train_model      gpu-node01  COMPLETED    0          00:15      2026-09-18 10:00:00 
```

---

## 🤖 Native AI Agent Skill Integration

Mini-Slurm includes an official agent specification at `skills/mini-slurm/SKILL.md` designed for LLM coding agents:

### How to use with Agents:
- **Antigravity**: Place `skills/mini-slurm` into `.gemini/antigravity/skills/` or your workspace skills directory.
- **Claude Code / Codex**: Add `skills/mini-slurm/SKILL.md` as an attached resource or system prompt guideline.

With this skill, an agent can autonomously:
1. Probe cluster GPU capacity (`sinfo`);
2. Select appropriate hardware constraints (`-C a100`, `-w node`);
3. Submit jobs and track Job IDs;
4. Periodically inspect output logs;
5. Detect failures and trigger retries or diagnostics.

---

## 🛡️ Automated Testing & Safety Verification

Mini-Slurm is verified by comprehensive chaos and edge-case test suites located in `tests/`:

```bash
# 1. Core Safety & Concurrency (Atomic reservation, idempotency, negative resource rejection)
python3 -B tests/test_safety.py

# 2. Fault Tolerance & Boundaries (SSH 255 disconnect protection, clock skew, process reaping)
python3 -B tests/test_reaudit.py

# 3. Process Probe Accuracy & Data Migration (Token-level /proc matching, prefix isolation, migration)
python3 -B tests/test_probe_and_kill.py
python3 -B tests/test_t3_t4.py

# 4. Physical GPU Telemetry & Anti-Collision (External occupancy detection, dynamic recovery)
python3 -B tests/test_hardware_awareness.py

# 5. Constraint Reconciliation & Node Validation (-C / --gres arbitration, telemetry Fail-Closed)
python3 -B tests/test_nodelist_constraint.py
```

---

## 📂 Repository Structure

```text
mini-slurm/
├── bin/                       # User CLI binaries
│   ├── sbatch                 # Submit batch jobs
│   ├── squeue                 # Query active/pending queue
│   ├── sinfo                  # Cluster and GPU status (-l for physical telemetry)
│   ├── scancel                # Job cancellation and process tree reaping
│   ├── sacct                  # Job accounting and audit history
│   └── mslurm                 # Daemon management (start/stop/restart/status/logs)
├── sbin/                      # System daemons
│   └── mslurm-sched           # Core scheduler daemon (reconciliation, probing, allocation)
├── libexec/                   # Worker executors
│   └── mslurm-executor        # Remote worker executor (heartbeat, tracking, clean exit)
├── lib/                       # Python internal libraries
│   ├── mslurm_db.py           # SQLite WAL engine (atomic transactions, CHECK constraints)
│   └── mslurm_parser.py       # SBATCH directive parser and argument normalization
├── etc/                       # Configuration files
│   └── nodes.json.example     # Cluster nodes and GPU topology template
├── examples/                  # Batch job script examples
│   ├── test_a100.sbatch       # A100 GPU test job
│   ├── test_rtx6000.sbatch    # RTX 6000 test job
│   ├── test_2080ti.sbatch     # RTX 2080Ti test job
│   ├── test_cpu.sbatch        # CPU-only test job
│   └── test_cancel.sbatch     # Job cancellation verification
├── tests/                     # Automated chaos and safety test suites
│   ├── test_safety.py         # Concurrency and reservation safety tests
│   ├── test_reaudit.py        # Network disconnect and monotonic clock tests
│   ├── test_probe_and_kill.py # Token-level argument matching tests
│   ├── test_t3_t4.py          # Early signal handling and data migration tests
│   ├── test_hardware_awareness.py  # Physical GPU sensing and anti-collision tests
│   └── test_nodelist_constraint.py # Constraint arbitration and node validation tests
├── skills/                    # AI Agent skills
│   └── mini-slurm/
│       └── SKILL.md           # Mini-Slurm Agent Skill specification
├── .gitignore
├── LICENSE                    # MIT License
├── README.md                  # Chinese documentation
└── README_EN.md               # English documentation
```

---

## 📄 License

Mini-Slurm is released under the [MIT License](LICENSE). Issues and Pull Requests are welcome!
