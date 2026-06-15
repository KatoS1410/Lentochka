# Lentochka DSMC

Tool for orchestrating backup workflows based on DSMC (IBM TSM client) in Linux environments.

The script scans repository structures, evaluates backup readiness, executes DSMC commands, and tracks processing state to avoid duplicate runs.

---

## Overview

Lentochka processes backup "stanzas" located in repository directories. Each stanza is validated through rsync status files and then passed to DSMC for backup or archive execution.

The tool is designed to run in controlled environments where:
- multiple repositories exist
- backups are processed incrementally
- execution must be protected from parallel runs
- logs and state must be preserved per run

---

## Features

### Backup orchestration
- Scans `.repo` directories under configured `search_root`
- Detects backup stanzas inside `backup/` folders
- Reads `rsync.status` to determine readiness:
  - `complete` → eligible for backup
  - `failed` → skipped
  - missing/invalid → logged and skipped

### DSMC execution
- Executes DSMC incremental backup commands
- Supports optional archive processing
- Captures stdout/stderr from DSMC
- Parses DSMC output to count backed-up objects

### State tracking
- Creates `lentochka-status` file per processed stanza
- Prevents duplicate processing of already completed stanzas

### Logging
- Global logs:
  - `global-lentochka.log`
  - `global-dsmc.log`
- Per-run logs (timestamped)
- DSMC session logs
- Iteration logs merged into global log
- Automatic log rotation (1GB threshold)
- Compression of rotated logs (.gz)

### Process control
- PID lock file to prevent concurrent execution
- Max instance limit enforcement (psutil-based)
- Automatic cleanup of stale lock files
- Graceful termination of previous processes if needed

### Monitoring integration
- External script-based metric reporting
- Sends basic metrics:
  - processed stanzas
  - failed stanzas
  - script errors
  - DSMC missing detection

### Cleanup
- Removes empty status logs
- Optional cleanup of old logs based on retention policy
- Removes empty `lentochka-status` directory if unused

---

## Configuration

The script uses an INI file: `LentochkaDSMC.ini`

### Required sections:

#### Paths

[Paths]
search_root = /path/to/repos
lock_file = /tmp/lentochka_dsmc.lock
lentochka_status_dir = ...


#### Logging

[Logging]
dsmc_log_dir = /var/log/dsmc
lentochka_log_dir = /var/log/lentochka
log_file = /var/log/lentochka/main.log
log_level = INFO
log_retention_days = 90
log_cleanup_enabled = true


#### DSMC

[DSMC]
dsmc_path = dsmc
dsmc_command_template = {dsmc_path} incremental "{backup_dirs}" -su=yes -se=ROV1-TSM -subdir=yes


#### Monitoring (optional)

[Monitoring]
enabled = true
monitoring_script = /path/to/script.sh
interval = 300


---

## Execution flow

1. Load configuration
2. Acquire process lock
3. Scan repository structure
4. Collect valid stanzas
5. Filter by rsync status
6. Skip already processed stanzas
7. Execute DSMC backup
8. Parse results from DSMC logs
9. Write `lentochka-status`
10. Cleanup logs and temporary data

---

## Requirements

- Python 3.8+
- DSMC (IBM Spectrum Protect / TSM client)
- psutil
- Linux environment recommended

---

## Notes

- Script is designed for long-running batch execution
- Relies heavily on filesystem state consistency
- Safe to re-run — already processed stanzas are skipped
- Lock file prevents parallel execution issues
