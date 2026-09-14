---
name: mini-slurm
description: Manage and schedule GPU and CPU jobs across a distributed cluster using Mini-Slurm (Slurm-compatible CLI over OpenSSH and shared storage). Use this skill when submitting batch scripts, checking node availability, querying queue status, inspecting job logs, or cancelling jobs.
---

# Mini-Slurm Cluster Management Skill

This skill guides AI coding assistants (such as Antigravity, Claude, Codex, etc.) to reliably interact with a **Mini-Slurm** lightweight cluster.

## Cluster Overview & Architecture

Mini-Slurm provides a Slurm-compatible user experience without requiring Slurm daemons, Redis, Ray, or root privileges.
- **Central Scheduler**: Runs on the primary head node (`mslurm-sched`), tracking queue and resource accounting via SQLite WAL.
- **Compute Nodes**: Ephemeral worker processes launched on demand via OpenSSH. No resident node agent required.
- **Storage**: Shared filesystem (e.g. `/mnt/share`) hosting job scripts, logs, status files, and stdout/stderr.

## Core Commands Reference

Mini-Slurm implements standard Slurm command semantics:

| Command | Purpose | Common Options / Usage |
| :--- | :--- | :--- |
| `sinfo` | View cluster nodes, health, CPUs, Memory, and GPU capacity | `sinfo` |
| `squeue` | Inspect running and pending jobs in the scheduling queue | `squeue` |
| `sbatch` | Submit a batch script to the queue | `sbatch <script.sbatch>` |
| `scancel` | Cancel a running or pending job | `scancel <job_id>` |
| `sacct` | Query job execution history, exit codes, and timestamps | `sacct` or `sacct -j <job_id>` |
| `mslurm` | Control scheduler daemon (start/stop/restart/status/logs) | `mslurm status`, `mslurm logs` |

## Job Script (`.sbatch`) Directives

Write job scripts with standard `#SBATCH` directives. Example:

```bash
#!/bin/bash
#SBATCH --job-name=train_model
#SBATCH --cpus-per-task=4
#SBATCH --mem=16000
#SBATCH --gres=gpu:1
#SBATCH -C a100
#SBATCH --time=01:30:00
#SBATCH --output=/mnt/share/logs/%j.out
#SBATCH --error=/mnt/share/logs/%j.err

# Mini-Slurm automatically sets CUDA_VISIBLE_DEVICES for GPU jobs
echo "Assigned GPU: $CUDA_VISIBLE_DEVICES"
python train.py --epochs 10
```

### Supported Directives
- `--job-name`, `-J`: Name of the job.
- `--cpus-per-task`, `-c`: Number of CPU cores required (e.g. `-c 4` or `--cpus-per-task=4`).
- `--mem`: Memory in MB required.
- `--gres=gpu:<count>`: Number of GPUs required (e.g. `--gres=gpu:1` or `--gres=gpu:a100:2`).
- `-C`, `--constraint`: Hardware model requirement (e.g. `-C a100`, `-C rtx6000`, `-C rtx2080ti`).
- `--time`, `-t`: Time limit in `HH:MM:SS`, `MM:SS`, or `D-HH:MM:SS`.
- `--output`, `-o`: Stdout redirection path (supports `%j` placeholder for job ID).
- `--error`, `-e`: Stderr redirection path (supports `%j` placeholder for job ID).
- `--chdir`, `-D`: Working directory on the compute node.

## Agent Workflow Guidelines

When an AI agent is asked to train a model, run experiments, or execute benchmarks on the cluster:

1. **Check Resources First**:
   Always run `sinfo` to check which nodes and GPUs are currently idle (`0/4` or `0/8` allocated).
2. **Select Constraints Appropriately**:
   - If training requires high VRAM (>40GB), request `-C a100` or target `a800`.
   - If running standard inference or fast testing, request `-C rtx6000` or `-C rtx2080ti`.
   - If running purely data preprocessing, omit `--gres` and only request `-c <cpus> --mem <mb>`.
3. **Submit with `sbatch`**:
   Submit the script and capture the printed Job ID:
   ```bash
   sbatch script.sbatch
   # Output: Submitted batch job 19
   ```
4. **Monitor Progress**:
   - Use `squeue` to confirm job transitions from `PENDING` -> `RUNNING`.
   - Read job output logs periodically in `/mnt/share/mini-slurm/jobs/<job_id>/slurm.out` or your custom `--output` path.
5. **Verify Clean Exit**:
   Once the job leaves `squeue`, run `sacct -j <job_id>` to confirm `COMPLETED` and `ExitCode 0`.
   If `ExitCode != 0` or state is `FAILED`/`TIMEOUT`, inspect `slurm.err` to diagnose issues.

## Fault Tolerance & Clean Termination

- When cancelling a job with `scancel <job_id>`, Mini-Slurm automatically terminates the entire session tree (including background child processes and `set -m` forks) on the compute node and atomically returns reserved GPUs to the pool.
- Never manually SSH into compute nodes to run `kill -9` unless Mini-Slurm scheduler is offline.
