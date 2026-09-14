import sqlite3
import json
import time
import os
import shutil

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    ssh_target TEXT,
    python_bin TEXT,
    total_cpus INTEGER,
    total_mem_mb INTEGER,
    gpu_count INTEGER,
    gpu_info TEXT,
    status TEXT DEFAULT 'UP',
    last_heartbeat REAL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    user TEXT,
    status TEXT DEFAULT 'PENDING',
    req_cpus INTEGER DEFAULT 1 CHECK (req_cpus >= 1),
    req_mem_mb INTEGER DEFAULT 8192 CHECK (req_mem_mb >= 1),
    req_gpus INTEGER DEFAULT 0 CHECK (req_gpus >= 0),
    gres_model TEXT,
    time_limit_sec INTEGER DEFAULT 0 CHECK (time_limit_sec >= 0),
    chdir TEXT,
    script_content TEXT,
    script_path TEXT,
    stdout_path TEXT,
    stderr_path TEXT,
    allocated_node TEXT,
    allocated_cpus INTEGER DEFAULT 0,
    allocated_gpus TEXT,
    remote_pid INTEGER,
    remote_pgid INTEGER,
    exit_code INTEGER,
    submit_time REAL,
    dispatch_time REAL,
    start_time REAL,
    end_time REAL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS gpu_allocations (
    node_name TEXT,
    gpu_id INTEGER,
    job_id INTEGER,
    PRIMARY KEY (node_name, gpu_id)
);

CREATE TABLE IF NOT EXISTS cpu_mem_allocations (
    node_name TEXT PRIMARY KEY,
    allocated_cpus INTEGER DEFAULT 0 CHECK (allocated_cpus >= 0),
    allocated_mem_mb INTEGER DEFAULT 0 CHECK (allocated_mem_mb >= 0)
);
"""

class MiniSlurmDB:
    def __init__(self, db_path="/home/lisy/mini-slurm/var/mslurm.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def init_db(self):
        conn = self.get_conn()
        try:
            with conn:
                # executescript commits pending transactions; keep recovery,
                # table creation and copying in one explicit transaction.
                conn.execute("BEGIN IMMEDIATE")
                for statement in SCHEMA.split(";"):
                    if statement.strip():
                        conn.execute(statement)
                self._migrate_schema(conn)
        finally:
            conn.close()

    def _migrate_schema(self, conn):
        # Recovery tables may contain the only copy of historical data.
        for table, old_table in (("jobs", "_jobs_old"), ("cpu_mem_allocations", "_cma_old")):
            old_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (old_table,)
            ).fetchone()
            if not old_exists:
                continue
            current_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            old_count = conn.execute(f"SELECT COUNT(*) FROM {old_table}").fetchone()[0]
            if current_count and old_count:
                raise RuntimeError(
                    f"Recovery conflict: {table} and {old_table} both contain rows; "
                    "both tables preserved. Reconcile them before retrying."
                )
            if old_count:
                conn.execute(f"DROP TABLE {table}")
                conn.execute(f"ALTER TABLE {old_table} RENAME TO {table}")
            else:
                conn.execute(f"DROP TABLE {old_table}")

        # 1. Ensure jobs table has CHECK constraints and dispatch_time
        jobs_row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
        if jobs_row and "CHECK" not in jobs_row[0].upper():
            col_rows = conn.execute("PRAGMA table_info(jobs)").fetchall()
            cols = [r[1] for r in col_rows]
            
            # Sanitize expressions for CHECK constraints so legacy bad data is normalized
            select_cols = []
            for c in cols:
                if c == 'req_cpus':
                    select_cols.append("MAX(1, req_cpus) AS req_cpus")
                elif c == 'req_mem_mb':
                    select_cols.append("MAX(1, req_mem_mb) AS req_mem_mb")
                elif c == 'req_gpus':
                    select_cols.append("MAX(0, req_gpus) AS req_gpus")
                elif c == 'time_limit_sec':
                    select_cols.append("MAX(0, time_limit_sec) AS time_limit_sec")
                else:
                    select_cols.append(c)
            col_list = ", ".join(cols)
            select_list = ", ".join(select_cols)

            conn.execute("ALTER TABLE jobs RENAME TO _jobs_old;")
            conn.execute("""
                CREATE TABLE jobs (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    user TEXT,
                    status TEXT DEFAULT 'PENDING',
                    req_cpus INTEGER DEFAULT 1 CHECK (req_cpus >= 1),
                    req_mem_mb INTEGER DEFAULT 8192 CHECK (req_mem_mb >= 1),
                    req_gpus INTEGER DEFAULT 0 CHECK (req_gpus >= 0),
                    gres_model TEXT,
                    time_limit_sec INTEGER DEFAULT 0 CHECK (time_limit_sec >= 0),
                    chdir TEXT,
                    script_content TEXT,
                    script_path TEXT,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    allocated_node TEXT,
                    allocated_cpus INTEGER DEFAULT 0,
                    allocated_gpus TEXT,
                    remote_pid INTEGER,
                    remote_pgid INTEGER,
                    exit_code INTEGER,
                    submit_time REAL,
                    dispatch_time REAL,
                    start_time REAL,
                    end_time REAL,
                    error_message TEXT
                );
            """)
            conn.execute(f"INSERT INTO jobs ({col_list}) SELECT {select_list} FROM _jobs_old;")
            conn.execute("DROP TABLE _jobs_old;")

        # Ensure dispatch_time column exists
        cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if "dispatch_time" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN dispatch_time REAL")

        # 2. Ensure cpu_mem_allocations has CHECK constraints
        cma_row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='cpu_mem_allocations'").fetchone()
        if cma_row and "CHECK" not in cma_row[0].upper():
            conn.execute("ALTER TABLE cpu_mem_allocations RENAME TO _cma_old;")
            conn.execute("""
                CREATE TABLE cpu_mem_allocations (
                    node_name TEXT PRIMARY KEY,
                    allocated_cpus INTEGER DEFAULT 0 CHECK (allocated_cpus >= 0),
                    allocated_mem_mb INTEGER DEFAULT 0 CHECK (allocated_mem_mb >= 0)
                );
            """)
            conn.execute("INSERT INTO cpu_mem_allocations (node_name, allocated_cpus, allocated_mem_mb) SELECT node_name, MAX(0, allocated_cpus), MAX(0, allocated_mem_mb) FROM _cma_old;")
            conn.execute("DROP TABLE _cma_old;")

    def sync_nodes(self, nodes_config):
        """
        Synchronizes nodes with nodes_config.
        Nodes marked enabled=False or removed from config will be set to DRAIN.
        """
        with self.get_conn() as conn:
            configured_names = set(nodes_config.keys())
            
            # 1. Update/insert configured nodes
            for name, cfg in nodes_config.items():
                is_enabled = cfg.get("enabled", True)
                st = "UP" if is_enabled else "DRAIN"
                gpus = cfg.get("gpus", [])
                conn.execute("""
                    INSERT INTO nodes (name, ssh_target, python_bin, total_cpus, total_mem_mb, gpu_count, gpu_info, status, last_heartbeat)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        ssh_target=excluded.ssh_target,
                        python_bin=excluded.python_bin,
                        total_cpus=excluded.total_cpus,
                        total_mem_mb=excluded.total_mem_mb,
                        gpu_count=excluded.gpu_count,
                        gpu_info=excluded.gpu_info,
                        status=excluded.status
                """, (
                    name,
                    cfg.get("ssh_target", name),
                    cfg.get("python_bin", "python3"),
                    cfg.get("cpus", 16),
                    cfg.get("mem_mb", 64000),
                    len(gpus),
                    json.dumps(gpus),
                    st,
                    time.time()
                ))
                conn.execute("""
                    INSERT INTO cpu_mem_allocations (node_name, allocated_cpus, allocated_mem_mb)
                    VALUES (?, 0, 0)
                    ON CONFLICT(node_name) DO NOTHING
                """, (name,))

            # 2. Nodes in DB but not in nodes_config -> DRAIN
            all_db_nodes = [r[0] for r in conn.execute("SELECT name FROM nodes").fetchall()]
            for db_n in all_db_nodes:
                if db_n not in configured_names:
                    conn.execute("UPDATE nodes SET status = 'DRAIN' WHERE name = ?", (db_n,))

    def submit_job(self, name, user, req_cpus, req_mem_mb, req_gpus, gres_model,
                   time_limit_sec, chdir, script_content, stdout_path, stderr_path):
        if req_cpus < 1:
            raise ValueError(f"req_cpus must be >= 1, got {req_cpus}")
        if req_mem_mb < 1:
            raise ValueError(f"req_mem_mb must be >= 1, got {req_mem_mb}")
        if req_gpus < 0:
            raise ValueError(f"req_gpus must be >= 0, got {req_gpus}")
        if time_limit_sec < 0:
            raise ValueError(f"time_limit_sec must be >= 0, got {time_limit_sec}")

        now = time.time()
        with self.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO jobs (
                    name, user, status, req_cpus, req_mem_mb, req_gpus, gres_model,
                    time_limit_sec, chdir, script_content, stdout_path, stderr_path,
                    submit_time
                ) VALUES (?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                name, user, req_cpus, req_mem_mb, req_gpus, gres_model,
                time_limit_sec, chdir, script_content, stdout_path, stderr_path, now
            ))
            job_id = cur.lastrowid
            return job_id

    def get_pending_jobs(self):
        with self.get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM jobs WHERE status = 'PENDING' ORDER BY submit_time ASC
            """).fetchall()
            return [dict(r) for r in rows]

    def try_allocate_job(self, job_id):
        """Atomic reservation of CPU, RAM, and GPU for a pending job."""
        with self.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            
            job = cur.execute("SELECT * FROM jobs WHERE job_id = ? AND status = 'PENDING'", (job_id,)).fetchone()
            if not job:
                return None
            
            req_cpus = job["req_cpus"]
            req_mem_mb = job["req_mem_mb"]
            req_gpus = job["req_gpus"]
            gres_model = job["gres_model"]

            # Only candidate nodes that are strictly UP
            nodes = cur.execute("SELECT * FROM nodes WHERE status = 'UP'").fetchall()
            
            for node in nodes:
                node_name = node["name"]
                total_cpus = node["total_cpus"]
                total_mem = node["total_mem_mb"]
                
                # Check current allocated CPU/RAM
                alloc = cur.execute("SELECT allocated_cpus, allocated_mem_mb FROM cpu_mem_allocations WHERE node_name = ?", (node_name,)).fetchone()
                cur_cpus = alloc["allocated_cpus"] if alloc else 0
                cur_mem = alloc["allocated_mem_mb"] if alloc else 0

                if (cur_cpus + req_cpus) > total_cpus:
                    continue
                if (cur_mem + req_mem_mb) > total_mem:
                    continue

                # Check GPUs if needed
                allocated_gpu_ids = []
                if req_gpus > 0:
                    node_gpus = json.loads(node["gpu_info"])
                    # Filter by model if requested (support a100/a800 interchangeable matching)
                    eligible_gpus = []
                    for g in node_gpus:
                        g_model = g["model"].lower().strip()
                        aliases = [a.lower().strip() for a in g.get("aliases", [])] + [g_model]
                        if not gres_model:
                            eligible_gpus.append(g["id"])
                        else:
                            req_m = gres_model.lower().strip()
                            # Exact match against model or configured aliases
                            matched = (req_m == g_model) or (req_m in aliases)
                            if matched:
                                eligible_gpus.append(g["id"])
                    
                    if len(eligible_gpus) < req_gpus:
                        continue
                    
                    # Find which ones are currently free
                    busy_gpus = [r[0] for r in cur.execute("SELECT gpu_id FROM gpu_allocations WHERE node_name = ?", (node_name,)).fetchall()]
                    free_gpus = [gid for gid in eligible_gpus if gid not in busy_gpus]
                    
                    if len(free_gpus) < req_gpus:
                        continue
                    allocated_gpu_ids = free_gpus[:req_gpus]

                # Commit reservation
                cur.execute("""
                    UPDATE cpu_mem_allocations
                    SET allocated_cpus = allocated_cpus + ?, allocated_mem_mb = allocated_mem_mb + ?
                    WHERE node_name = ?
                """, (req_cpus, req_mem_mb, node_name))

                for gid in allocated_gpu_ids:
                    cur.execute("""
                        INSERT INTO gpu_allocations (node_name, gpu_id, job_id)
                        VALUES (?, ?, ?)
                    """, (node_name, gid, job_id))

                now = time.time()
                gpu_str = ",".join(str(g) for g in allocated_gpu_ids) if allocated_gpu_ids else ""
                cur.execute("""
                    UPDATE jobs
                    SET status = 'STARTING', allocated_node = ?, allocated_cpus = ?, allocated_gpus = ?, dispatch_time = ?
                    WHERE job_id = ?
                """, (node_name, req_cpus, gpu_str, now, job_id))

                conn.commit()
                return {
                    "job_id": job_id,
                    "node_name": node_name,
                    "ssh_target": node["ssh_target"],
                    "python_bin": node["python_bin"],
                    "allocated_gpus": allocated_gpu_ids,
                    "allocated_cpus": req_cpus,
                    "allocated_mem_mb": req_mem_mb,
                    "dispatch_time": now
                }
            return None

    def mark_job_running(self, job_id, pid, pgid):
        now = time.time()
        with self.get_conn() as conn:
            conn.execute("""
                UPDATE jobs
                SET status = 'RUNNING', remote_pid = ?, remote_pgid = ?, start_time = ?
                WHERE job_id = ? AND status = 'STARTING'
            """, (pid, pgid, now, job_id))

    def request_cancel(self, job_id):
        """
        Safely initiates cancellation:
        - If PENDING: cancels immediately and releases nothing (no resources held).
        - If STARTING/RUNNING: marks status='CANCEL_REQUESTED' (keeps resources reserved until confirmed).
        - If terminal: no-op.
        Returns: (action, job_dict) where action is 'CANCELLED', 'CANCEL_REQUESTED', or 'TERMINATED'
        """
        now = time.time()
        with self.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            job = cur.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if not job:
                return None, None
            
            st = job["status"]
            if st in ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"):
                return "TERMINATED", dict(job)
            
            if st == "PENDING":
                cur.execute("UPDATE jobs SET status = 'CANCELLED', exit_code = 130, end_time = ? WHERE job_id = ?", (now, job_id))
                conn.commit()
                return "CANCELLED", dict(job)
            
            # STARTING or RUNNING
            cur.execute("UPDATE jobs SET status = 'CANCEL_REQUESTED' WHERE job_id = ?", (job_id,))
            conn.commit()
            return "CANCEL_REQUESTED", dict(job)

    def finish_job(self, job_id, final_status, exit_code, error_msg=None):
        """
        Strictly idempotent job completion & resource release.
        Only transitions from active states ('PENDING', 'STARTING', 'RUNNING', 'CANCEL_REQUESTED').
        Guarantees resources are released EXACTLY ONCE.
        """
        now = time.time()
        with self.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            job = cur.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if not job:
                return False
            
            current_st = job["status"]
            # IDEMPOTENCY GUARD: if already in a terminal state, DO NOT touch allocations!
            if current_st in ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"):
                return False

            node_name = job["allocated_node"]
            cpus = job["allocated_cpus"] or 0
            mem = job["req_mem_mb"] or 0
            
            # Release resources strictly once
            if node_name:
                cur.execute("""
                    UPDATE cpu_mem_allocations
                    SET allocated_cpus = MAX(0, allocated_cpus - ?),
                        allocated_mem_mb = MAX(0, allocated_mem_mb - ?)
                    WHERE node_name = ?
                """, (cpus, mem, node_name))
                cur.execute("DELETE FROM gpu_allocations WHERE job_id = ?", (job_id,))

            cur.execute("""
                UPDATE jobs
                SET status = ?, exit_code = ?, error_message = ?, end_time = ?
                WHERE job_id = ?
            """, (final_status, exit_code, error_msg, now, job_id))
            conn.commit()
            return True

    def get_job(self, job_id):
        with self.get_conn() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self, active_only=False, limit=50):
        with self.get_conn() as conn:
            if active_only:
                rows = conn.execute("""
                    SELECT * FROM jobs WHERE status IN ('PENDING', 'STARTING', 'RUNNING', 'CANCEL_REQUESTED')
                    ORDER BY job_id ASC
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM jobs ORDER BY job_id DESC LIMIT ?
                """, (limit,)).fetchall()
            return [dict(r) for r in rows]

    def list_nodes(self):
        with self.get_conn() as conn:
            nodes = conn.execute("SELECT * FROM nodes ORDER BY name ASC").fetchall()
            res = []
            for n in nodes:
                d = dict(n)
                d["gpus"] = json.loads(d["gpu_info"])
                alloc = conn.execute("SELECT allocated_cpus, allocated_mem_mb FROM cpu_mem_allocations WHERE node_name = ?", (d["name"],)).fetchone()
                d["allocated_cpus"] = alloc["allocated_cpus"] if alloc else 0
                d["allocated_mem_mb"] = alloc["allocated_mem_mb"] if alloc else 0
                busy_gpus = [r[0] for r in conn.execute("SELECT gpu_id FROM gpu_allocations WHERE node_name = ?", (d["name"],)).fetchall()]
                d["busy_gpus"] = busy_gpus
                res.append(d)
            return res

    def backup(self, backup_path):
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with self.get_conn() as conn:
            bck = sqlite3.connect(backup_path)
            with bck:
                conn.backup(bck)
            bck.close()
