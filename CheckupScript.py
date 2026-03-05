#!/usr/bin/env python3
"""
CheckupScript – автономный инструмент для проверки готовности LentochkaDSMC.py.

Скрипт полностью автономен: сам ищет dsmc, конфиги и проверяет настройки.
Не требует .ini файла и ничего не изменяет в системе.

Проверки выполняются последовательно:
1. Поиск исполняемого dsmc по всей системе.
2. Сверка версии dsmc с ожидаемой (8.1.7) и проверка наличия критичных флагов.
3. Поиск и анализ конфигов dsmc (dsm.sys, dsm.opt) на конфликты.
4. Проверка настроек планировщика (scheduler) на предмет конфликтов.
5. Создание тестовой станзы в /tmp, запись её на ленту и обратное восстановление
   с проверкой md5-сумм.

Весь ход выполнения пишется в CheckUpLentochka.log (debug-режим). По завершении
выводится компактное резюме по всем шагам.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

EXPECTED_DSMC_VERSION = "8.1.7"
LOG_FILE = Path(__file__).with_name("CheckUpLentochka.log")

# Стандартные места для поиска dsmc конфигов
DSM_CONFIG_PATHS = [
    Path("/opt/tivoli/tsm/client/ba/bin/dsm.sys"),
    Path("/usr/tivoli/tsm/client/ba/bin/dsm.sys"),
    Path.home() / ".dsm.sys",
    Path("/etc/dsm.sys"),
    Path("/opt/tivoli/tsm/client/ba/bin/dsm.opt"),
    Path("/usr/tivoli/tsm/client/ba/bin/dsm.opt"),
    Path.home() / ".dsm.opt",
    Path("/etc/dsm.opt"),
]


@dataclass
class CheckResult:
    name: str
    status: str  # OK / WARN / FAIL
    message: str


class CheckupScript:
    def __init__(self):
        self.logger = logging.getLogger("Checkup")
        self.logger.setLevel(logging.DEBUG)
        self._setup_logging()
        self.summary: List[CheckResult] = []
        self.dsmc_path: Optional[str] = None
        self.dsm_config_files: List[Path] = []

    def _setup_logging(self) -> None:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def _run_command(
        self, args: List[str], timeout: int = 300, shell: bool = False
    ) -> Tuple[int, str, str]:
        self.logger.debug("Executing command: %s", " ".join(args) if not shell else args)
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
            shell=shell,
        )
        self.logger.debug("Command exited with %s", proc.returncode)
        if proc.stdout:
            self.logger.debug("STDOUT:\n%s", proc.stdout.strip())
        if proc.stderr:
            self.logger.debug("STDERR:\n%s", proc.stderr.strip())
        return proc.returncode, proc.stdout, proc.stderr

    def _find_dsmc_path(self) -> Optional[str]:
        """Ищет dsmc по всей системе: PATH, which, find, locate."""
        self.logger.info("Searching for dsmc binary...")

        # 1. Проверка PATH через which
        if shutil.which("dsmc"):
            path = shutil.which("dsmc")
            self.logger.info("DSMC found in PATH: %s", path)
            self.summary.append(
                CheckResult("dsmc_presence", "OK", f"Found DSMC in PATH at {path}")
            )
            return path

        # 2. Поиск через find в стандартных местах
        self.logger.debug("Searching with find in standard locations...")
        search_paths = ["/usr/bin", "/usr/local/bin", "/opt", "/usr/tivoli"]
        for search_path in search_paths:
            if not Path(search_path).exists():
                continue
            try:
                rc, stdout, _ = self._run_command(
                    ["find", search_path, "-name", "dsmc", "-type", "f", "-executable"],
                    timeout=30,
                )
                if rc == 0 and stdout.strip():
                    paths = [p.strip() for p in stdout.strip().split("\n") if p.strip()]
                    if paths:
                        path = paths[0]
                        self.logger.info("DSMC found via find: %s", path)
                        self.summary.append(
                            CheckResult(
                                "dsmc_presence",
                                "OK",
                                f"Found DSMC via find at {path}",
                            )
                        )
                        return path
            except Exception as e:
                self.logger.debug("find search in %s failed: %s", search_path, e)

        # 3. Поиск через locate (если доступен)
        self.logger.debug("Trying locate command...")
        try:
            rc, stdout, _ = self._run_command(["locate", "-b", "\\dsmc"], timeout=30)
            if rc == 0 and stdout.strip():
                paths = [
                    p.strip()
                    for p in stdout.strip().split("\n")
                    if p.strip() and Path(p.strip()).is_file()
                    and os.access(p.strip(), os.X_OK)
                ]
                if paths:
                    path = paths[0]
                    self.logger.info("DSMC found via locate: %s", path)
                    self.summary.append(
                        CheckResult(
                            "dsmc_presence",
                            "OK",
                            f"Found DSMC via locate at {path}",
                        )
                    )
                    return path
        except Exception as e:
            self.logger.debug("locate search failed: %s", e)

        # 4. Поиск в типичных местах установки TSM
        self.logger.debug("Checking typical TSM installation paths...")
        typical_paths = [
            "/opt/tivoli/tsm/client/ba/bin/dsmc",
            "/usr/tivoli/tsm/client/ba/bin/dsmc",
            "/opt/IBM/spectrumprotect/bin/dsmc",
            "/usr/bin/dsmc",
            "/usr/local/bin/dsmc",
        ]
        for path_str in typical_paths:
            path_obj = Path(path_str)
            if path_obj.exists() and path_obj.is_file() and os.access(path_str, os.X_OK):
                self.logger.info("DSMC found at typical path: %s", path_str)
                self.summary.append(
                    CheckResult(
                        "dsmc_presence",
                        "OK",
                        f"Found DSMC at typical path {path_str}",
                    )
                )
                return path_str

        message = "Unable to locate dsmc binary. Searched: PATH, find, locate, typical paths."
        self.logger.error(message)
        self.summary.append(CheckResult("dsmc_presence", "FAIL", message))
        return None

    def _find_dsm_configs(self) -> List[Path]:
        """Ищет конфиги dsm.sys и dsm.opt по всей системе."""
        self.logger.info("Searching for dsmc configuration files...")
        found_configs: List[Path] = []

        # 1. Проверка стандартных путей
        for config_path in DSM_CONFIG_PATHS:
            if config_path.exists() and config_path.is_file():
                self.logger.debug("Found config at standard path: %s", config_path)
                found_configs.append(config_path)

        # 2. Поиск через find (ограничиваем поиск разумными местами)
        self.logger.debug("Searching for dsm.sys and dsm.opt with find...")
        search_roots = ["/opt", "/usr", "/etc", str(Path.home())]
        for config_name in ["dsm.sys", "dsm.opt"]:
            for search_root in search_roots:
                if not Path(search_root).exists():
                    continue
                try:
                    rc, stdout, stderr = self._run_command(
                        [
                            "find",
                            search_root,
                            "-name",
                            config_name,
                            "-type",
                            "f",
                        ],
                        timeout=30,
                    )
                    if rc == 0 and stdout.strip():
                        for line in stdout.strip().split("\n"):
                            line = line.strip()
                            if line and Path(line).exists():
                                path = Path(line)
                                if path not in found_configs:
                                    found_configs.append(path)
                                    self.logger.debug("Found config via find: %s", path)
                except Exception as e:
                    self.logger.debug("find search for %s in %s failed: %s", config_name, search_root, e)

        # 3. Поиск через locate
        for config_name in ["dsm.sys", "dsm.opt"]:
            try:
                rc, stdout, _ = self._run_command(
                    ["locate", "-b", config_name], timeout=30
                )
                if rc == 0 and stdout.strip():
                    for line in stdout.strip().split("\n"):
                        line = line.strip()
                        if line and Path(line).exists() and Path(line).is_file():
                            path = Path(line)
                            if path not in found_configs:
                                found_configs.append(path)
                                self.logger.debug("Found config via locate: %s", path)
            except Exception as e:
                self.logger.debug("locate search for %s failed: %s", config_name, e)

        if found_configs:
            self.logger.info(
                "Found %d dsmc config file(s): %s",
                len(found_configs),
                ", ".join(str(p) for p in found_configs),
            )
            self.summary.append(
                CheckResult(
                    "dsm_configs",
                    "OK",
                    f"Found {len(found_configs)} config file(s): "
                    + ", ".join(str(p) for p in found_configs),
                )
            )
        else:
            message = "No dsmc configuration files (dsm.sys, dsm.opt) found."
            self.logger.warning(message)
            self.summary.append(CheckResult("dsm_configs", "WARN", message))

        return found_configs

    def _parse_dsm_config(self, config_path: Path) -> dict:
        """Парсит dsm.sys или dsm.opt файл."""
        result = {"file": str(config_path), "sections": {}, "issues": []}
        current_section = None

        try:
            with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("*"):
                        continue

                    # Секции в формате [section_name]
                    if line.startswith("[") and line.endswith("]"):
                        current_section = line[1:-1].strip()
                        result["sections"][current_section] = {}
                        continue

                    # Параметры в формате key = value или key value
                    if "=" in line:
                        key, value = line.split("=", 1)
                    elif " " in line:
                        parts = line.split(None, 1)
                        if len(parts) == 2:
                            key, value = parts
                        else:
                            continue
                    else:
                        continue

                    key = key.strip().lower()
                    value = value.strip()

                    if current_section:
                        result["sections"][current_section][key] = value
                    else:
                        if "global" not in result["sections"]:
                            result["sections"]["global"] = {}
                        result["sections"]["global"][key] = value

        except Exception as e:
            result["issues"].append(f"Error parsing {config_path}: {e}")

        return result

    def _check_dsm_configs_for_conflicts(self) -> None:
        """Проверяет конфиги на настройки, которые могут конфликтовать с LentochkaDSMC."""
        if not self.dsm_config_files:
            return

        self.logger.info("Analyzing dsmc configuration files for conflicts...")
        conflicts = []

        for config_path in self.dsm_config_files:
            self.logger.debug("Analyzing config: %s", config_path)
            parsed = self._parse_dsm_config(config_path)

            # Проверка на scheduler включен
            for section_name, section_data in parsed["sections"].items():
                # Проверка параметров, связанных с планировщиком
                scheduler_keys = [
                    "schedmode",
                    "schedlogretention",
                    "schedlogname",
                    "schedname",
                ]
                for key in scheduler_keys:
                    if key in section_data:
                        value = section_data[key].lower()
                        if value in ["yes", "enabled", "1", "true"]:
                            conflicts.append(
                                f"{config_path}: section [{section_name}] has {key}={section_data[key]} "
                                "(scheduler may interfere with on-demand backups)"
                            )

                # Проверка на автоматический запуск
                if "autostart" in section_data:
                    value = section_data["autostart"].lower()
                    if value in ["yes", "enabled", "1", "true"]:
                        conflicts.append(
                            f"{config_path}: section [{section_name}] has autostart enabled "
                            "(may interfere with on-demand backups)"
                        )

        if conflicts:
            message = "Potential conflicts found in dsmc configs: " + "; ".join(conflicts)
            status = "WARN"
        else:
            message = "No obvious conflicts found in dsmc configuration files."
            status = "OK"

        self.summary.append(CheckResult("dsm_config_conflicts", status, message))
        getattr(self.logger, "info" if status == "OK" else "warning")(message)

    def check_version_and_flags(self) -> None:
        if not self.dsmc_path:
            return

        # Version
        rc, stdout, stderr = self._run_command([self.dsmc_path, "-version"])
        output = stdout or stderr
        if rc != 0 or not output:
            message = "Failed to obtain DSMC version."
            self.summary.append(CheckResult("dsmc_version", "FAIL", message))
            self.logger.error(message)
        else:
            if EXPECTED_DSMC_VERSION in output:
                message = f"DSMC version matches expected {EXPECTED_DSMC_VERSION}"
                status = "OK"
            else:
                message = (
                    f"Detected DSMC version differs from expected {EXPECTED_DSMC_VERSION}: "
                    f"{output.splitlines()[0] if output.splitlines() else 'unknown'}"
                )
                status = "WARN"
            self.summary.append(CheckResult("dsmc_version", status, message))
            getattr(self.logger, "info" if status == "OK" else "warning")(message)

        # Flags
        rc, stdout, stderr = self._run_command(
            [self.dsmc_path, "help", "incremental"], timeout=120
        )
        help_text = stdout or stderr
        required_flags = ["-subdir", "-replace"]
        missing = [
            flag for flag in required_flags if flag not in (help_text or "").lower()
        ]
        if rc != 0 or not help_text:
            message = "Failed to inspect DSMC incremental help output."
            status = "WARN"
        elif missing:
            message = (
                "Missing expected flags in DSMC incremental help: "
                + ", ".join(missing)
            )
            status = "WARN"
        else:
            message = "Required DSMC flags (-subdir, -replace) are available."
            status = "OK"
        self.summary.append(CheckResult("dsmc_flags", status, message))
        getattr(self.logger, "info" if status == "OK" else "warning")(message)

    def check_scheduler(self) -> None:
        if not self.dsmc_path:
            return

        rc, stdout, stderr = self._run_command(
            [self.dsmc_path, "query", "sched"], timeout=120
        )
        if rc != 0:
            message = (
                "Failed to query DSMC scheduler configuration."
                f" Return code {rc}. STDERR: {stderr.strip()}"
            )
            self.summary.append(CheckResult("scheduler", "WARN", message))
            self.logger.warning(message)
            return

        schedules = self._parse_scheduler_output(stdout)
        if not schedules:
            message = "Scheduler entries not found. Nothing conflicts with on-demand backups."
            status = "OK"
        else:
            message = (
                "Scheduler entries detected: "
                + "; ".join(
                    f"{s.get('name', 'unknown')} ({s.get('action', 'n/a')})"
                    for s in schedules
                )
            )
            status = "WARN"
        self.summary.append(CheckResult("scheduler", status, message))
        getattr(self.logger, "info" if status == "OK" else "warning")(message)

    @staticmethod
    def _parse_scheduler_output(output: str) -> List[dict]:
        schedules: List[dict] = []
        current: dict = {}
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                if current:
                    schedules.append(current)
                    current = {}
                continue
            if line.lower().startswith("schedule name"):
                current["name"] = line.split(":", 1)[-1].strip()
            elif line.lower().startswith("action"):
                current["action"] = line.split(":", 1)[-1].strip()
            elif line.lower().startswith("status"):
                current["status"] = line.split(":", 1)[-1].strip()
        if current:
            schedules.append(current)
        return schedules

    def run_diagnostic_backup(self) -> None:
        if not self.dsmc_path:
            self.logger.warning("Skipping diagnostic backup: dsmc not found")
            return

        temp_root = Path("/tmp")
        staging_dir = temp_root / f"lentochka-checkup-{datetime.datetime.now():%Y%m%d%H%M%S}"
        staging_dir.mkdir(parents=True, exist_ok=True)

        try:
            test_file = staging_dir / "test_payload.bin"
            test_content = os.urandom(1024 * 4)
            test_file.write_bytes(test_content)
            original_hash = self._md5_for_path(staging_dir)
            self.logger.info("Created test stanza at %s (md5=%s)", staging_dir, original_hash)

            backup_cmd = [
                self.dsmc_path,
                "incremental",
                str(staging_dir),
                "-subdir=yes",
                "-replace=no",
            ]
            rc, _, _ = self._run_command(backup_cmd, timeout=600)
            if rc != 0:
                message = (
                    f"Diagnostic backup failed with return code {rc}. "
                    "See log for details."
                )
                self.summary.append(CheckResult("diagnostic_backup", "FAIL", message))
                self.logger.error(message)
                return

            # подчистим локальную копию, чтобы restore записал данные заново в ту же директорию
            shutil.rmtree(staging_dir)
            staging_dir.mkdir(parents=True, exist_ok=True)

            restore_cmd = [
                self.dsmc_path,
                "restore",
                f"{staging_dir}/*",
                "-subdir=yes",
                "-replace=yes",
            ]
            rc, _, _ = self._run_command(restore_cmd, timeout=600)
            if rc != 0:
                message = (
                    f"Diagnostic restore failed with return code {rc}. "
                    "See log for details."
                )
                self.summary.append(CheckResult("diagnostic_restore", "FAIL", message))
                self.logger.error(message)
                return

            restored_hash = self._md5_for_path(staging_dir)
            if original_hash == restored_hash:
                message = (
                    f"Diagnostic stanza backed up and restored successfully "
                    f"(md5={original_hash})."
                )
                status = "OK"
            else:
                message = (
                    "Mismatch between original and restored data. "
                    f"Original md5={original_hash}, restored md5={restored_hash}."
                )
                status = "FAIL"
            self.summary.append(CheckResult("diagnostic_roundtrip", status, message))
            getattr(self.logger, "info" if status == "OK" else "error")(message)
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _md5_for_path(self, base_path: Path) -> str:
        digest = hashlib.md5()
        for root, _, files in os.walk(base_path):
            for file_name in sorted(files):
                file_path = Path(root) / file_name
                relative = file_path.relative_to(base_path)
                digest.update(str(relative).encode("utf-8"))
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        digest.update(chunk)
        return digest.hexdigest()

    def run(self) -> None:
        try:
            self.dsmc_path = self._find_dsmc_path()
            if not self.dsmc_path:
                self.logger.error("Cannot proceed without dsmc binary")
                return

            self.check_version_and_flags()
            self.dsm_config_files = self._find_dsm_configs()
            self._check_dsm_configs_for_conflicts()
            self.check_scheduler()
            self.run_diagnostic_backup()
        except Exception as exc:
            self.logger.exception("Checkup aborted: %s", exc)
            self.summary.append(
                CheckResult("checkup_aborted", "FAIL", f"{exc.__class__.__name__}: {exc}")
            )
        finally:
            self.print_summary()

    def print_summary(self) -> None:
        self.logger.info("=" * 60)
        self.logger.info("CHECKUP SUMMARY")
        for result in self.summary:
            self.logger.info("[%s] %s: %s", result.status, result.name, result.message)
        self.logger.info("Log saved to %s", LOG_FILE)
        self.logger.info("=" * 60)


def main() -> None:
    checkup = CheckupScript()
    checkup.run()


if __name__ == "__main__":
    main()
