# Mini-Slurm 🚀

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Standard%20Library%20Only-brightgreen.svg)]()
[![Slurm Compatible](https://img.shields.io/badge/CLI-Slurm%20Compatible-orange.svg)]()
[![Battle Tested](https://img.shields.io/badge/Tests-3%20Audit%20Rounds%20Passed-success.svg)]()

**Mini-Slurm** 是一个专为中小规模多机 CPU / GPU 计算集群设计的轻量级、免 Root、免常驻节点守护进程的分布式作业调度系统。

它使用纯 **Python 标准库 + SQLite WAL + OpenSSH + 共享存储（NFS/Ceph/SMB）** 构建，提供与传统 Slurm 完全一致的交互体验（`sbatch`、`squeue`、`sinfo`、`scancel`、`sacct`），同时原生配备了供 AI 编程助手（如 Antigravity, Claude, Codex）直接调用的 **Agent Skill**。

---

## 🌟 核心特性 (Key Features)

- **纯标准库零外部依赖**：仅依赖 Python 3.8+ 标准库（`sqlite3`, `subprocess`, `fcntl`, `shlex`, `json` 等），无需安装任何第三方 pip 包。
- **计算节点零驻留守护进程**：计算节点无需运行 `slurmd`、Redis 或 RPC 服务，调度器通过原生 OpenSSH 按需拉起轻量级执行器（`mslurm-executor`），任务结束后自动退出。
- **Slurm 风格 CLI 兼容**：
  - `sbatch`：支持 `#SBATCH` 指令解析（`-c`, `--mem`, `--gres=gpu:N`, `-C/--constraint`, `--time`, `-o`, `-e` 等）。
  - `squeue`：实时查看就绪、排队与运行中作业及其绑定的 GPU 卡号。
  - `sinfo`：查看节点硬件状态（UP/DOWN/DRAIN）、CPU、内存与 GPU 实时分配率。
  - `scancel`：信号级作业取消，联动远端会话树递归杀灭。
  - `sacct`：作业历史耗时、退出码与状态溯源。
  - `mslurm`：一键管理调度服务（`start` / `stop` / `restart` / `status` / `logs`）。
- **多用户物理防踩踏与透明硬件感知 (Hardware Awareness & Anti-Collision)**：专为共享计算集群设计！调度器通过多线程并发探测远端 `nvidia-smi` 物理显存；他人直接登录裸跑占用的卡自动标记为 `EXT_BUSY` 并主动避让；`sinfo` 支持概要显示 `GPUS(A/E/I/T)`（已分配/外部占用/完全空闲/总数），`sinfo -l` 详细透视每张卡的显存使用量与归属。
- **极度严苛的安全与并发设计（历经三轮专家混沌审计）**：
  - **时钟漂移免疫**：采用单调递增序列号（`heartbeat_seq`）与调度端本地单调时钟比对，彻底免疫节点间数小时级的墙钟偏差。
  - **网络异常资源冻结**：SSH 探测返回 255（网络不通/节点故障）时，调度器严格保持 GPU 预留，必须经正向探针证实无存活进程后才允许回收。
  - **Token 级进程探针**：基于 `/proc/*/cmdline` 的 NUL 字节严格分词匹配，杜绝自身匹配（Self-matching）与作业号前缀串扰（Prefix bleed，如取消 Job 1 误杀 Job 10/11）。
  - **会话与进程树彻底清理**：子进程前置注册信号处理器；退出与取消时递归扫描 `/proc` 清理整棵进程树与 `set -m` 作业控制子进程，杜绝后台孤儿进程。
  - **数据库强约束与平滑迁移**：内置 SQLite WAL 模式、排他文件锁与 `CHECK` 约束，杜绝负数资源请求与超分，并具备脏数据自动归一化迁移机制。
- **AI Agent 原生集成**：内置 `skills/mini-slurm/SKILL.md`，可无缝挂载至 Antigravity、Claude Code、Codex 等 AI 智能体，实现全自主实验派发与日志监控。

---

## 🏗️ 架构设计 (Architecture)

```mermaid
flowchart TD
    User["你 / AI Agent (Antigravity / Codex)"]
    User -->|sbatch / squeue / scancel / sinfo / sacct| HeadNode["174 主控节点 (Head Node)"]

    subgraph HeadNode["174 主控节点"]
        Sched["mslurm-sched (守护进程)"]
        DB[(SQLite WAL<br/>账本与作业队列)]
        Lock["mslurm-sched.lock<br/>flock 独占单实例"]
        Sched <--> DB
        Sched --- Lock
    end

    HeadNode -->|OpenSSH (按需无常驻 Agent)| WorkerA["计算节点 A (如 a800: A100-SXM4-80GB)"]
    HeadNode -->|OpenSSH (按需无常驻 Agent)| WorkerB["计算节点 B (如 172share: RTX 6000 Ada)"]
    HeadNode -->|OpenSSH (按需无常驻 Agent)| WorkerC["计算节点 C (如 176: RTX 2080Ti)"]

    subgraph SharedStorage["共享存储 (如 /mnt/share)"]
        Exec["libexec/mslurm-executor"]
        JobsDir["jobs/{job_id}/<br/>- job.sh<br/>- slurm.out / slurm.err<br/>- .status.json<br/>- .cancel"]
    end

    WorkerA <--> SharedStorage
    WorkerB <--> SharedStorage
    WorkerC <--> SharedStorage
```

---

## 🚀 快速上手 (Quick Start)

### 1. 前置条件

1. **操作系统**：Linux（推荐 Ubuntu 20.04/22.04+ 或 Debian/CentOS/RHEL）。
2. **Python**：Python 3.8 或更高版本（主控节点与所有计算节点均具备）。
3. **SSH 免密互信**：主控节点可通过 `ssh <node_target>` 免密登录所有计算节点。
4. **共享目录**：主控节点与计算节点挂载了相同的共享目录（如 `/mnt/share`）。

### 2. 目录结构配置

建议在主控节点或共享存储上解压 Mini-Slurm：

```bash
git clone https://github.com/your-username/mini-slurm.git /mnt/share/mini-slurm
cd /mnt/share/mini-slurm
```

将可执行命令加入环境变量（例如在 `~/.bashrc` 中添加）：
```bash
export PATH="/mnt/share/mini-slurm/bin:$PATH"
```

### 3. 配置集群节点 (`etc/nodes.json`)

复制模板文件并按实际集群配置硬件：
```bash
cp etc/nodes.json.example etc/nodes.json
vim etc/nodes.json
```

配置示例：
```json
{
  "nodes": {
    "node-a100": {
      "ssh_target": "a800",
      "python_bin": "/opt/conda/bin/python",
      "cpus": 24,
      "mem_mb": 120000,
      "gpus": [
        {"id": 0, "model": "a100", "aliases": ["a100", "a800", "a100-80gb"]},
        {"id": 1, "model": "a100", "aliases": ["a100", "a800", "a100-80gb"]}
      ],
      "enabled": true
    },
    "node-rtx": {
      "ssh_target": "172share",
      "python_bin": "/usr/bin/python3",
      "cpus": 16,
      "mem_mb": 64000,
      "gpus": [
        {"id": 0, "model": "rtx6000", "aliases": ["rtx6000", "blackwell"]}
      ],
      "enabled": true
    }
  }
}
```

### 4. 启动调度服务

```bash
# 启动调度守护进程
mslurm start

# 检查服务状态
mslurm status

# 查看集群状态
sinfo
```

输出示例：
```text
NODELIST     STATE    CPUS(A/I/T)    MEM(A/T MB)        GPUS(A/T)    GPU_MODELS          
node-rtx     UP       0/16/16        0/64000            0/1          rtx6000             
node-a100    UP       0/24/24        0/120000           0/2          a100                
```

---

## 📝 作业提交与管理示例

### 1. 提交 GPU 作业 (`run_gpu.sbatch`)

```bash
#!/bin/bash
#SBATCH --job-name=deepseek_test
#SBATCH --cpus-per-task=4
#SBATCH --mem=16000
#SBATCH --gres=gpu:1
#SBATCH -C a100
#SBATCH --time=01:00:00
#SBATCH --output=/mnt/share/logs/%j.out
#SBATCH --error=/mnt/share/logs/%j.err

echo "Node: $(hostname)"
echo "Assigned GPU device: $CUDA_VISIBLE_DEVICES"
python3 train.py
```

提交与查询：
```bash
$ sbatch run_gpu.sbatch
Submitted batch job 1

$ squeue
JOBID    NAME             USER       ST         TIME       NODELIST(RESOURCES)     
1        deepseek_test    lisy       RUNNING    00:05      node-a100:gpu[0]        
```

### 2. 提交纯 CPU 作业

```bash
#!/bin/bash
#SBATCH --job-name=data_prep
#SBATCH --cpus-per-task=8
#SBATCH --mem=32000
#SBATCH --time=00:30:00

python3 preprocess.py
```

### 3. 取消作业

```bash
$ scancel 1
Cancellation requested for job 1 on node-a100
```
Mini-Slurm 会立即向远端执行器注入取消信号，并在数秒内完成整棵进程树强制杀灭与 GPU/CPU 账本原子回收。

### 4. 查看历史与计费

```bash
$ sacct
JobID    JobName          Node       State        ExitCode   Elapsed    SubmitTime          
1        deepseek_test    node-a100  COMPLETED    0          00:15      2026-09-15 08:00:00 
```

---

## 🤖 AI Agent Skill 原生集成

Mini-Slurm 仓库自带 `skills/mini-slurm/SKILL.md`，专门为各种主流 AI 智能体设计：

### 如何挂载使用？
- **Antigravity**：将 `skills/mini-slurm` 放置在 `.gemini/antigravity/skills/` 或工作区 skills 目录下即可自动识别。
- **Claude Code / Codex**：将 `skills/mini-slurm/SKILL.md` 内容添加至系统 Prompt 或作为上下文参考文件。

智能体在拥有该 Skill 后，可自主完成：
1. 提交前自动探测集群卡闲置情况（`sinfo`）；
2. 根据训练脚本显存需求自动选择硬件约束（如 `-C a100`）；
3. 提交任务并记录 Job ID；
4. 周期性轮询与查看输出日志；
5. 在训练异常中断时触发重试或取消。

---

## 🛡️ 混沌测试与安全性验证 (Auditing & Tests)

Mini-Slurm 在设计过程中接受了三轮严格的对抗式自动化审计测试，内置了完备的测试套件（位于 `tests/` 目录）：

```bash
# 运行第一轮基线 11 项用例测试（并发争抢、幂等结算、负资源拒绝、空格指令兼容等）
python3 -B tests/test_safety.py

# 运行第二轮安全 10 项测试（SSH 255 网络断连保护、时钟漂移心跳、set -m 进程清理、CHECK 约束等）
python3 -B tests/test_reaudit.py

# 运行第三轮专项目测试（Token 级 /proc 探针防自匹配、前缀隔离、早期 SIGTERM 防孤儿、旧数据安全迁移）
python3 -B tests/test_probe_and_kill.py
python3 -B tests/test_t3_t4.py
```

---

## 📂 代码目录概览

```text
mini-slurm/
├── bin/                       # 用户 CLI 命令
│   ├── sbatch                 # 提交批处理作业
│   ├── squeue                 # 查询排队与运行状态
│   ├── sinfo                  # 集群与显卡硬件信息
│   ├── scancel                # 作业取消
│   ├── sacct                  # 作业历史与退出状态审计
│   └── mslurm                 # 守护进程管理脚本 (start/stop/restart/status/logs)
├── sbin/                      # 系统守护进程
│   └── mslurm-sched           # 核心调度器 (协调、对账、探针、资源分配)
├── libexec/                   # 远端计算节点执行器
│   └── mslurm-executor        # 任务执行器 (环境配置、心跳、进程组追踪、超时与取消治理)
├── lib/                       # Python 核心底层库
│   ├── mslurm_db.py           # SQLite WAL 存储引擎 (原子性账本、CHECK 约束、平滑迁移)
│   └── mslurm_parser.py       # SBATCH 语法解析器与参数规范化
├── etc/                       # 配置文件
│   └── nodes.json.example     # 节点与显卡硬件拓扑配置模板
├── examples/                  # 典型作业脚本示例
│   ├── test_a100.sbatch       # A100 GPU 测试作业
│   ├── test_rtx6000.sbatch    # RTX 6000 测试作业
│   ├── test_2080ti.sbatch     # RTX 2080Ti 测试作业
│   ├── test_cpu.sbatch        # 纯 CPU 测试作业
│   └── test_cancel.sbatch     # 作业取消测试脚本
├── tests/                     # 自动化安全与混沌测试套件
│   ├── test_safety.py         # 第一轮 11 项安全性测试
│   ├── test_reaudit.py        # 第二轮 10 项安全与边界测试
│   ├── test_probe_and_kill.py # 进程参数数组精确匹配专项测试
│   └── test_t3_t4.py          # 极速信号处理与脏数据热迁移测试
├── skills/                    # AI Agent 智能体技能库
│   └── mini-slurm/
│       └── SKILL.md           # Mini-Slurm Agent Skill 规范文件
├── .gitignore
├── LICENSE                    # MIT 开源协议
└── README.md                  # 项目中英文说明文档
```

---

## 📄 License

Mini-Slurm 采用 [MIT License](LICENSE) 开源协议。欢迎提交 Issue 与 Pull Request！
