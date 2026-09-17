import re
import os
import shlex

def parse_memory_mb(mem_str):
    if not mem_str:
        return 8192
    mem_str = str(mem_str).strip().upper()
    m = re.match(r"^(\d+)\s*([KMGT]?B?)$", mem_str)
    if not m:
        raise ValueError(f"Invalid memory format: {mem_str}")
    val = int(m.group(1))
    if val <= 0:
        raise ValueError(f"Memory must be positive, got: {mem_str}")
    unit = m.group(2).replace("B", "")
    if unit in ("", "M"):
        return val
    elif unit == "K":
        return max(1, val // 1024)
    elif unit == "G":
        return val * 1024
    elif unit == "T":
        return val * 1024 * 1024
    return val

def parse_time_seconds(time_str):
    if not time_str:
        return 0
    time_str = str(time_str).strip()
    
    # 1. Day-based formats: D-HH:MM:SS, D-HH:MM, D-HH
    if "-" in time_str:
        parts = time_str.split("-", 1)
        if not parts[0].isdigit():
            raise ValueError(f"Invalid days in time format: {time_str}")
        days = int(parts[0])
        subparts = parts[1].split(":")
        if len(subparts) == 3: # D-HH:MM:SS
            if not all(p.isdigit() for p in subparts):
                raise ValueError(f"Invalid time format: {time_str}")
            h, m, s = int(subparts[0]), int(subparts[1]), int(subparts[2])
            if h >= 24 or m >= 60 or s >= 60:
                raise ValueError(f"Time values out of range in: {time_str}")
            return days * 86400 + h * 3600 + m * 60 + s
        elif len(subparts) == 2: # D-HH:MM
            if not all(p.isdigit() for p in subparts):
                raise ValueError(f"Invalid time format: {time_str}")
            h, m = int(subparts[0]), int(subparts[1])
            if h >= 24 or m >= 60:
                raise ValueError(f"Time values out of range in: {time_str}")
            return days * 86400 + h * 3600 + m * 60
        elif len(subparts) == 1: # D-HH
            if not subparts[0].isdigit():
                raise ValueError(f"Invalid time format: {time_str}")
            h = int(subparts[0])
            if h >= 24:
                raise ValueError(f"Hours out of range in: {time_str}")
            return days * 86400 + h * 3600
        else:
            raise ValueError(f"Invalid time format: {time_str}")

    # 2. Colon-based formats: HH:MM:SS or MM:SS
    if ":" in time_str:
        subparts = time_str.split(":")
        if not all(p.isdigit() for p in subparts):
            raise ValueError(f"Invalid time format: {time_str}")
        if len(subparts) == 3: # HH:MM:SS
            h, m, s = int(subparts[0]), int(subparts[1]), int(subparts[2])
            if m >= 60 or s >= 60:
                raise ValueError(f"Minutes or seconds out of range in: {time_str}")
            return h * 3600 + m * 60 + s
        elif len(subparts) == 2: # MM:SS
            m, s = int(subparts[0]), int(subparts[1])
            if s >= 60:
                raise ValueError(f"Seconds out of range in: {time_str}")
            return m * 60 + s
        else:
            # e.g. 1:2:3:4 - invalid!
            raise ValueError(f"Invalid time format (too many colons): {time_str}")

    # 3. Pure integer minutes
    if time_str.isdigit():
        mins = int(time_str)
        if mins < 0:
            raise ValueError(f"Time must be non-negative: {time_str}")
        return mins * 60

    raise ValueError(f"Unrecognized time format: {time_str}")

def parse_gres(gres_str):
    if not gres_str:
        return 0, None
    parts = gres_str.strip().split(":")
    if parts[0].lower() != "gpu":
        raise ValueError(f"Only 'gpu' GRES is supported, got: {parts[0]}")
    if len(parts) == 2:
        if not parts[1].isdigit():
            raise ValueError(f"Invalid GPU count in --gres: {parts[1]}")
        count = int(parts[1])
        if count < 0:
            raise ValueError(f"GPU count must be non-negative, got: {count}")
        return count, None
    elif len(parts) == 3:
        model = parts[1].lower()
        if not parts[2].isdigit():
            raise ValueError(f"Invalid GPU count in --gres: {parts[2]}")
        count = int(parts[2])
        if count < 0:
            raise ValueError(f"GPU count must be non-negative, got: {count}")
        return count, model
    else:
        raise ValueError(f"Invalid --gres format: {gres_str}")

def parse_sbatch_script(script_path_or_content, is_content=False, default_chdir=None):
    if is_content:
        lines = script_path_or_content.splitlines()
        content = script_path_or_content
    else:
        with open(script_path_or_content, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        lines = content.splitlines()

    cwd = default_chdir or os.getcwd()
    safe_chdir = cwd if cwd.startswith("/mnt/share") else "/mnt/share"

    params = {
        "job_name": "sbatch_job",
        "cpus_per_task": 1,
        "mem_mb": 8192,
        "gpus": 0,
        "gres_model": None,
        "constraint": None,
        "nodelist": None,
        "time_sec": 0,
        "chdir": safe_chdir,
        "stdout": None,
        "stderr": None,
    }

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#!"):
            continue
        if not stripped.startswith("#SBATCH"):
            if not stripped.startswith("#"):
                break
            continue

        directive_text = stripped[7:].strip()
        try:
            tokens = shlex.split(directive_text)
        except Exception as e:
            raise ValueError(f"Syntax error in #SBATCH directive '{directive_text}': {e}")

        i = 0
        while i < len(tokens):
            tok = tokens[i]
            
            # Helper to get value either from --key=val, next token, or -kVal
            def get_val(tok, flag, next_i):
                if tok.startswith(flag + "="):
                    return tok[len(flag) + 1:], next_i
                elif tok == flag:
                    if next_i >= len(tokens):
                        raise ValueError(f"Option {flag} requires an argument")
                    return tokens[next_i], next_i + 1
                elif len(flag) == 2 and flag.startswith("-") and tok.startswith(flag) and len(tok) > 2 and not tok.startswith("--"):
                    return tok[2:], next_i
                return None, next_i

            # Check each supported option
            val, next_i = get_val(tok, "--job-name", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-J", i + 1)
            if val is not None:
                params["job_name"] = val
                i = next_i
                continue

            val, next_i = get_val(tok, "--cpus-per-task", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-c", i + 1)
            if val is not None:
                try:
                    c = int(val)
                except ValueError:
                    raise ValueError(f"Invalid integer for cpus-per-task: {val}")
                if c <= 0:
                    raise ValueError(f"--cpus-per-task must be >= 1, got: {c}")
                params["cpus_per_task"] = c
                i = next_i
                continue

            val, next_i = get_val(tok, "--mem", i + 1)
            if val is not None:
                params["mem_mb"] = parse_memory_mb(val)
                i = next_i
                continue

            val, next_i = get_val(tok, "--gres", i + 1)
            if val is not None:
                g, model = parse_gres(val)
                params["gpus"] = g
                if model is not None:
                    params["gres_model"] = model.lower()
                i = next_i
                continue

            val, next_i = get_val(tok, "--time", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-t", i + 1)
            if val is not None:
                params["time_sec"] = parse_time_seconds(val)
                i = next_i
                continue

            val, next_i = get_val(tok, "--chdir", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-D", i + 1)
            if val is not None:
                params["chdir"] = val
                i = next_i
                continue

            val, next_i = get_val(tok, "--output", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-o", i + 1)
            if val is not None:
                params["stdout"] = val
                i = next_i
                continue

            val, next_i = get_val(tok, "--error", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-e", i + 1)
            if val is not None:
                params["stderr"] = val
                i = next_i
                continue

            val, next_i = get_val(tok, "--nodes", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-N", i + 1)
            if val is not None:
                if int(val) != 1:
                    raise ValueError(f"Mini-Slurm only supports --nodes=1 in v1, got: {val}")
                i = next_i
                continue

            val, next_i = get_val(tok, "--ntasks", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-n", i + 1)
            if val is not None:
                if int(val) != 1:
                    raise ValueError(f"Mini-Slurm only supports --ntasks=1 in v1, got: {val}")
                i = next_i
                continue

            val, next_i = get_val(tok, "--constraint", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-C", i + 1)
            if val is not None:
                val = val.lower().strip()
                if not val:
                    raise ValueError("Option --constraint/-C cannot be empty")
                params["constraint"] = val
                i = next_i
                continue

            val, next_i = get_val(tok, "--nodelist", i + 1)
            if val is None:
                val, next_i = get_val(tok, "-w", i + 1)
            if val is not None:
                val = val.strip()
                if not val:
                    raise ValueError("Option --nodelist/-w cannot be empty")
                params["nodelist"] = val
                i = next_i
                continue

            # If we reached here, unrecognized option!
            raise ValueError(f"Unknown or unsupported #SBATCH directive: {tok}")

    params["gres_model"] = reconcile_hardware_constraints(params.get("constraint"), params.get("gres_model"))
    return params, content

def reconcile_hardware_constraints(constraint, gres_model):
    """
    Reconciles -C/--constraint and --gres GPU model requirements.
    If both are specified, they must be compatible (e.g. identical or alias matches).
    Raises ValueError if they conflict.
    Returns the unified model string or None.
    """
    if not constraint and not gres_model:
        return None
    if constraint and not gres_model:
        return constraint.lower().strip()
    if gres_model and not constraint:
        return gres_model.lower().strip()

    c = constraint.lower().strip()
    g = gres_model.lower().strip()
    ALIAS_GROUPS = [
        {"a100", "a800", "a100-80gb", "a100-sxm4-80gb"},
        {"rtx2080ti", "2080ti"},
        {"rtx6000", "blackwell"}
    ]
    compatible = (c == g)
    if not compatible:
        for group in ALIAS_GROUPS:
            if c in group and g in group:
                compatible = True
                break
    if not compatible:
        raise ValueError(f"Conflicting GPU model requirements: --constraint={c} vs --gres={g}")
    return c
