
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
import gzip
import glob
import os
import sys
import signal
import logging
import subprocess
import configparser
import time
import psutil
import shutil
import datetime
monitoring = None
class DsmcPlusLentochkaLogs:
    def __init__(self, config_file: Optional[str] = None):
        try:
            self.log_manager = logging.getLogger('log_manager')
            self.config_file = config_file or self.find_config_file()
            print(f"Using config file: {self.config_file}")
            self.config = self.load_config(self.config_file)
            self.search_root = self.config.get('Paths', 'search_root')
            self.lentochka_status_dir = self.config.get('Paths', 'lentochka_status_dir', fallback='')
            self.dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir')
            self.lentochka_log_dir = self.config.get('Logging', 'lentochka_log_dir')
            self.dsmc_command_template = self.config.get('DSMC', 'dsmc_command_template')
            self.log_file = self.config.get('Logging', 'log_file')
            self.script = self.config.get('Monitoring', 'monitoring_script', fallback=None)
            if not self.search_root:
                self.log_manager.error("ERROR: 'search_root' parameter is missing or empty in the configuration.")
                raise ValueError("'search_root' must be specified in the configuration file.")
            if not self.dsmc_log_dir:
                self.log_manager.error("ERROR: 'dsmc_log_dir' parameter is missing in the configuration.")
                raise ValueError("'dsmc_log_dir' must be specified in the configuration file.")
            if not self.lentochka_log_dir:
                self.log_manager.error("ERROR: 'lentochka_log_dir' parameter is missing in the configuration.")
                raise ValueError("'lentochka_log_dir' must be specified in the configuration file.")
            if not self.dsmc_command_template:
                self.log_manager.error("ERROR: 'dsmc_command_template' parameter is missing in the configuration.")
                raise ValueError("'dsmc_command_template' must be specified in the configuration file.")
            if not self.log_file:
                self.log_manager.error("ERROR: 'log_file' parameter is missing in the configuration.")
                raise ValueError("'log_file' must be specified in the configuration file.")
            self._ensure_log_directories()
            log_level = self.config.get('Logging', 'log_level', fallback='INFO').upper()
            log_level = getattr(logging, log_level, logging.INFO)
            self.log_manager.setLevel(log_level)
            self._setup_lentochka_logger()
            self._setup_dsmc_logger()
            self.log_manager.info("DsmcPlusLentochkaLogs initialized successfully")
            self.lentochka_logger.info("Lentochka logging system initialized")
            self.dsmc_logger.info("DSMC logging system initialized")
        except Exception as exception:
            print(f"Error during initialization: {exception}")
            raise
    def _ensure_log_directories(self):
        for directory_key, directory in [
            ('lentochka_log_dir', self.lentochka_log_dir),
            ('dsmc_log_dir', self.dsmc_log_dir),
            ('lentochka_status_dir', self.lentochka_status_dir)
        ]:
            if directory:  
                if not os.path.isabs(directory):
                    config_dir = os.path.dirname(os.path.abspath(self.config_file))
                    abs_directory = os.path.join(config_dir, directory)
                    setattr(self, directory_key, abs_directory)
                    directory = abs_directory
                if not os.path.exists(directory):
                    os.makedirs(directory, exist_ok=True)  
                    print(f"Created directory: {directory}")
            else:
                print(f"Skipping creation of {directory_key} as it is not specified in config.")
    @staticmethod
    def find_config_file() -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(current_dir, 'LentochkaDSMC.ini')
        if os.path.exists(config_file):
            return config_file
        possible_paths = [
            os.path.join(os.path.expanduser('~'), 'LentochkaDSMC.ini'),
            '/etc/LentochkaDSMC.ini'
        ]
        for path in possible_paths:
            if os.path.exists(path):
                return path
        raise FileNotFoundError("Configuration file not found. Please create LentochkaDSMC.ini in script directory.")
    @staticmethod
    def load_config(config_file: str) -> configparser.ConfigParser:
        if not os.path.exists(config_file):
            error_msg = f"Configuration file not found: {config_file}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    def check_write_access(self, directory: str) -> bool:
        if not os.access(directory, os.W_OK):
            self.log_manager.error(f"No write access to directory: {directory}")
            return False
        return True
    def _setup_lentochka_logger(self):
        log_dir = self.lentochka_log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.lentochka_logger = logging.getLogger('lentochka')
        self.lentochka_logger.handlers = []
        self.lentochka_log_file = os.path.join(log_dir, 'global-lentochka.log')
        rotated_file = self.rotate_log(self.lentochka_log_file)
        if rotated_file:
            self.archive_log(rotated_file)
        handler = logging.FileHandler(self.lentochka_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.lentochka_logger.addHandler(handler)
        self.lentochka_logger.setLevel(logging.DEBUG)
        self.log_manager.info(f"Logging for Lentochka initialized in file: {self.lentochka_log_file}")
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.current_iteration_log_file = os.path.join(log_dir, f'lentochka-log-{timestamp}.log')
        self.iteration_handler = logging.FileHandler(self.current_iteration_log_file)
        self.iteration_handler.setFormatter(formatter)
        self.lentochka_logger.addHandler(self.iteration_handler)
        self.log_manager.info(f"Iteration log for Lentochka created at: {self.current_iteration_log_file}")
    def _setup_dsmc_logger(self):
        log_dir = self.dsmc_log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.dsmc_logger = logging.getLogger('dsmc')
        self.dsmc_logger.handlers = []
        self.dsmc_log_file = os.path.join(log_dir, 'global-dsmc.log')
        rotated_file = self.rotate_log(self.dsmc_log_file)
        if rotated_file:
            self.archive_log(rotated_file)
        handler = logging.FileHandler(self.dsmc_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.dsmc_logger.addHandler(handler)
        self.dsmc_logger.setLevel(logging.DEBUG)
        self.log_manager.info(f"Logging for DSMC initialized in file: {self.dsmc_log_file}")
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.current_dsmc_session_log_file = os.path.join(log_dir, f'dsmc-session-{timestamp}.log')
        session_handler = logging.FileHandler(self.current_dsmc_session_log_file)
        session_handler.setFormatter(formatter)
        self.dsmc_logger.addHandler(session_handler)
        self.dsmc_session_handler = session_handler
        self.log_manager.info(f"Session log for DSMC created at: {self.current_dsmc_session_log_file}")
    def log_lentochka_info(self, message):
        self.lentochka_logger.info(message)
    def log_lentochka_error(self, message):
        self.lentochka_logger.error(message)
    def log_dsmc_info(self, message):
        self.dsmc_logger.info(message)
    def log_dsmc_error(self, message):
        self.dsmc_logger.error(message)
    def append_dsmc_log_to_global(self, log_file_path):
        try:
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
                self.log_dsmc_info(f"--- Begin DSMC log {os.path.basename(log_file_path)} ---")
                for line in log_content.splitlines():
                    self.dsmc_logger.info(line)
                self.log_dsmc_info(f"--- End DSMC log {os.path.basename(log_file_path)} ---")
                self.log_manager.info(f"DSMC log appended to global log: {log_file_path}")
                return True
        except Exception as e:
            self.log_manager.error(f"Error reading DSMC log file: {e}")
            return False
    def rotate_log(self, log_file: str) -> Optional[str]:
        max_size = 1_073_741_824
        if not os.path.exists(log_file) or os.path.getsize(log_file) < max_size:
            return None
        log_dir = os.path.dirname(log_file)
        log_base = os.path.basename(log_file)
        n = 1
        while True:
            rotated_file = os.path.join(log_dir, f"{log_base}.{n}")
            if not os.path.exists(rotated_file) and not os.path.exists(f"{rotated_file}.gz"):
                break
            n += 1
            if n > 1000:
                self.log_manager.error("Infinite loop in rotate_log, too many rotated files, stopping.")
                return None
        try:
            for handler in self.lentochka_logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == os.path.abspath(log_file):
                    handler.close()
                    self.lentochka_logger.removeHandler(handler)
            for handler in self.dsmc_logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == os.path.abspath(log_file):
                    handler.close()
                    self.dsmc_logger.removeHandler(handler)
            os.rename(log_file, rotated_file)
            self.log_manager.info(f"Rotated log file: {log_file} -> {rotated_file}")
            return rotated_file
        except PermissionError as e:
            self.log_manager.error(f"***** WARNING: PERMISSION ERROR IN ROTATE_LOG {log_file}: {e} *****")
            return None
        except Exception as e:
            self.log_manager.error(f"Error rotating log file {log_file}: {e}")
            return None
    def archive_log(self, rotated_file: str) -> bool:
        if not rotated_file or not os.path.exists(rotated_file):
            return False
        try:
            gz_file = f"{rotated_file}.gz"
            with open(rotated_file, 'rb') as f_in:
                with gzip.open(gz_file, 'wb') as f_out:
                    while True:
                        chunk = f_in.read(8192)
                        if not chunk:
                            break
                        f_out.write(chunk)
            os.remove(rotated_file)
            self.log_manager.info(f"Archived log file: {rotated_file} -> {gz_file}")
            return True
        except PermissionError as e:
            self.log_manager.error(f"***** WARNING: PERMISSION ERROR IN ARCHIVE_LOG {rotated_file}: {e} *****")
            return False
        except Exception as e:
            self.log_manager.error(f"Error archiving log file {rotated_file}: {e}")
            return False
    def close_iteration_log(self):
        if hasattr(self, 'iteration_handler') and self.iteration_handler:
            try:
                self.iteration_handler.close()
                self.lentochka_logger.removeHandler(self.iteration_handler)
                if os.path.exists(self.current_iteration_log_file) and os.path.getsize(
                        self.current_iteration_log_file) > 0:
                    with open(self.current_iteration_log_file, 'r') as temp_log:
                        log_content = temp_log.read()
                        with open(self.lentochka_log_file, 'r') as check_log:
                            existing_content = check_log.read()
                            if log_content not in existing_content:
                                with open(self.lentochka_log_file, 'a') as global_log:
                                    global_log.write(
                                        f"\n--- Begin Iteration Log {os.path.basename(self.current_iteration_log_file)} ---\n")
                                    global_log.write(log_content)
                                    global_log.write(
                                        f"\n--- End Iteration Log {os.path.basename(self.current_iteration_log_file)} ---\n")
                self.log_manager.info(
                    f"Iteration log closed and appended to global log: {self.current_iteration_log_file}")
            except Exception as e:
                self.log_manager.error(f"Error closing iteration log: {e}")
    def validate_dsmc_log_dir(self):
        log_dir = self.dsmc_log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            self.log_manager.info(f"Created DSMC log directory: {log_dir}")
        return True
    def validate_lentochka_log_dir(self):
        log_dir = self.lentochka_log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            self.log_manager.info(f"Created Lentochka log directory: {log_dir}")
        return True
    def cleanup_empty_logs(self):
        if not self.lentochka_status_dir:
            return
        try:
            if not os.path.exists(self.lentochka_status_dir):
                return  
            for log_file in os.listdir(self.lentochka_status_dir):
                if log_file.endswith('.log'):
                    log_path = os.path.join(self.lentochka_status_dir, log_file)
                    if os.path.getsize(log_path) == 0:
                        os.remove(log_path)
                        self.log_manager.info(f'Deleted empty log file: {log_path}')
        except Exception as e:
            self.log_manager.error(f"Error during log cleanup: {e}")
class MonitoringHandler:
    def __init__(self, config, log_manager):
        self.config = config
        self.log_manager = log_manager
        self.enabled = config.getboolean('Monitoring', 'enabled', fallback=False)
        self.script = config.get('Monitoring', 'monitoring_script', fallback=None)
        self.interval = config.getint('Monitoring', 'interval', fallback=300)
        self.log_dir = config.get('Paths', 'log_dir', fallback='logs')
        self.log_retention_days = config.getint('Logging', 'log_retention_days', fallback=90)
        self.log_cleanup_enabled = config.getboolean('Logging', 'log_cleanup_enabled', fallback=True)
        search_root = config.get('Paths', 'search_root')
        self.log_manager.info(f"Search directory specified in .ini file: {search_root}")
    @staticmethod
    def sanitize_metric_name(name):
        return name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    def send_metric(self, metric_name, value, status='OK'):
        if not self.enabled or not self.script:
            self.log_manager.warning("Monitoring is disabled or monitoring script is not set.")
            return
        try:
            sanitized_name = self.sanitize_metric_name(metric_name)
            cmd = [self.script, sanitized_name, str(value), status]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.log_manager.info(f"Metric sent: {sanitized_name} with value: {value} and status: {status}")
        except subprocess.CalledProcessError as error:
            self.log_manager.error(f"Error sending metric to monitoring: {error}")
    def cleanup_logs(self, log_dir, log_retention_days):
        if not self.log_cleanup_enabled:
            self.log_manager.info("Automatic log cleanup is disabled.")
            return 0
        if not log_dir:
            self.log_manager.warning("Log directory not specified in configuration. Skipping cleanup.")
            return 0
        if not os.path.isabs(log_dir):
            config_dir = os.path.dirname(os.path.abspath(self.config['Paths']['config_file']))
            log_dir = os.path.join(config_dir, log_dir)
        if not os.path.isdir(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
                self.log_manager.info(f"Created log directory: {log_dir}")
            except Exception as e:
                self.log_manager.warning(f"Cannot create log directory {log_dir}: {e}")
                return 0
        deleted_files_count = 0
        for log_file in os.listdir(log_dir):
            log_file_path = os.path.join(log_dir, log_file)
            if os.path.isfile(log_file_path):
                file_age_days = (datetime.datetime.now() - datetime.datetime.fromtimestamp(
                    os.path.getmtime(log_file_path))).days
                if file_age_days > log_retention_days:
                    try:
                        os.remove(log_file_path)
                        deleted_files_count += 1
                    except Exception as file_error:
                        self.log_manager.error(f"Error removing file {log_file_path}: {file_error}")
        if deleted_files_count > 0:
            self.log_manager.info(f"Deleted {deleted_files_count} old logs.")
        return deleted_files_count
    def log_error_with_metrics(self, message, error):
        self.log_manager.error(f"{message}: {error}")
        try:
            if self.enabled and self.script:
                self.send_metric("error", 1, "ERROR")
        except Exception as send_error:
            self.log_manager.error(f"Error sending metrics: {send_error}")
class ProcessLocker:
    def __init__(self, lock_file_path, log_manager, max_instances):
        self.lock_file_path = lock_file_path
        self.pid_file = None
        self.log_manager = log_manager
        self.max_instances = max_instances
    def _find_existing_process(self):
        try:
            if not os.path.exists(self.lock_file_path):
                return None
            with open(self.lock_file_path, 'r') as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                return pid
            except OSError:
                return None
        except (IOError, ValueError) as read_error:
            self.log_manager.error(f"Error reading PID from file {self.lock_file_path}: {read_error}")
            return None
    def is_process_running(self):
        current_pid = os.getpid()
        running_processes = 0
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['pid'] != current_pid and proc.info['name'] == 'python3':
                cmdline = proc.cmdline()
                if len(cmdline) > 0 and 'LentochkaDSMC.py' in cmdline:
                    running_processes += 1
        if running_processes >= self.max_instances:
            self.log_manager.warning(f"Max instances reached ({self.max_instances}). Process cannot be started.")
            return True
        return False
    def terminate_existing_process(self):
        pid = self._find_existing_process()
        if pid is not None:
            self.log_manager.warning(f"Found active process with PID {pid}. Terminating.")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)
                try:
                    os.kill(pid, signal.SIGKILL)
                    self.log_manager.warning(f"Process {pid} did not terminate, forcing termination.")
                except ProcessLookupError:
                    self.log_manager.info(f"Process {pid} already terminated.")
                if os.path.exists(self.lock_file_path):
                    os.unlink(self.lock_file_path)
            except Exception as terminate_error:
                self.log_manager.error(f"Error terminating process {pid}: {terminate_error}")
        else:
            self.log_manager.info("No active processes to terminate.")
    def is_stale_lock(self):
        if os.path.exists(self.lock_file_path):
            try:
                with open(self.lock_file_path, 'r') as f:
                    pid = int(f.read().strip())
                try:
                    os.kill(pid, 0)
                    return False
                except OSError:
                    return True
            except (IOError, ValueError):
                return True
        return False
    def __enter__(self):
        if self.is_stale_lock():
            self.log_manager.warning("Stale lock file found, removing it.")
            try:
                os.remove(self.lock_file_path)
            except Exception as remove_error:
                self.log_manager.error(f"Error removing stale lock file: {remove_error}")
        if self.is_process_running():
            raise RuntimeError("Another instance of the process is already running.")
        self.terminate_existing_process()
        lock_file = self.lock_file_path
        lock_dir = os.path.dirname(lock_file)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir)
        if os.path.exists(lock_file):
            os.remove(lock_file)
        self.pid_file = open(lock_file, 'w')
        self.pid_file.write(str(os.getpid()))
        self.pid_file.flush()
        self.log_manager.info(f"Process lock acquired with PID {os.getpid()}")
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.pid_file:
                self.pid_file.close()
            if os.path.exists(self.lock_file_path):
                os.unlink(self.lock_file_path)
            self.log_manager.info("Process lock released.")
        except Exception as release_error:
            self.log_manager.error(f"Error releasing resources: {release_error}")
class StanzaProcessor:
    def __init__(self, config, lentochka_log):
        self.config = config
        self.lentochka_log = lentochka_log
        self.log_manager = lentochka_log.log_manager
        self.lentochka_log.validate_dsmc_log_dir()
        self.lentochka_log.validate_lentochka_log_dir()
    def find_stanzas(self) -> List[Dict[str, Any]]:
        stanzas = []
        search_root = Path(self.config.get('Paths', 'search_root'))
        rsync_status_count = {'total': 0, 'completed': 0, 'failed': 0, 'missing': 0}
        lentochka_status_count = {'total': 0}
        repo_status = {}
        for repo_dir in search_root.glob('*.repo'):
            repo_path = str(repo_dir)
            backup_dir = repo_dir / 'backup'
            if not backup_dir.exists():
                self.lentochka_log.log_lentochka_error(f"Backup directory not found: {backup_dir}")
                continue
            has_failed = False
            for rsync_status_path in backup_dir.rglob('rsync.status'):
                rsync_status_count['total'] += 1
                rsync_dir = rsync_status_path.parent
                lentochka_status_path = rsync_dir / 'lentochka-status'
                if lentochka_status_path.exists():
                    lentochka_status_count['total'] += 1
                try:
                    with open(rsync_status_path, 'r') as f:
                        status_content = f.read().strip().lower()
                        if 'failed' in status_content:
                            rsync_status_count['failed'] += 1
                            has_failed = True
                        elif 'complete' in status_content:
                            rsync_status_count['completed'] += 1
                        else:
                            rsync_status_count['missing'] += 1
                            stanza_name = rsync_dir.name
                            self.lentochka_log.log_lentochka_error(
                                f"MISSING STATUS: Repo '{repo_path}', stanza '{stanza_name}': "
                                f"rsync.status at {rsync_status_path} has status '{status_content}' "
                                f"(neither 'complete' nor 'failed'). "
                                f"Action: Check if rsync process is still running, or verify file contents manually. "
                                f"This stanza will be skipped until status becomes 'complete'."
                            )
                except IOError as exception:
                    rsync_status_count['missing'] += 1
                    stanza_name = rsync_dir.name
                    self.lentochka_log.log_lentochka_error(
                        f"MISSING STATUS (READ ERROR): Repo '{repo_path}', stanza '{stanza_name}': "
                        f"Failed to read rsync.status at {rsync_status_path}. "
                        f"Error: {exception}. "
                        f"Action: Check file permissions and ensure rsync process completed. "
                        f"This stanza will be skipped."
                    )
            repo_status[repo_path] = has_failed
        self.lentochka_log.log_lentochka_info(
            f"RESULTS: Found {rsync_status_count['total']} rsync.status files. "
            f"Completed (ready for tape): {rsync_status_count['completed']}, "
            f"failed (skipped): {rsync_status_count['failed']}, "
            f"missing/unknown status (skipped, see errors above): {rsync_status_count['missing']}. "
            f"Already processed (lentochka-status found): {lentochka_status_count['total']}"
        )
        for repo_dir in search_root.glob('*.repo'):
            repo_path = str(repo_dir)
            backup_dir = repo_dir / 'backup'
            if not backup_dir.exists():
                continue
            if repo_status.get(repo_path, False):
                self.lentochka_log.log_lentochka_info(
                    f"Skipping entire repo {repo_path} due to at least one failed rsync.status")
                continue
            for rsync_status_path in backup_dir.rglob('rsync.status'):
                rsync_dir = rsync_status_path.parent
                lentochka_status_path = rsync_dir / 'lentochka-status'
                if lentochka_status_path.exists():
                    self.lentochka_log.log_lentochka_info(
                        f"Stanza already processed: {repo_path} (at {lentochka_status_path})")
                    continue
                try:
                    with open(rsync_status_path, 'r') as f:
                        status_content = f.read().strip().lower()
                        if 'failed' in status_content:
                            continue
                        if 'complete' in status_content:
                            archive_dir = repo_dir / 'archive' / rsync_dir.name
                            stanza = {
                                'status_path': str(rsync_status_path),
                                'repo_path': repo_path,
                                'backup_path': str(rsync_dir),
                                'archive_path': str(archive_dir),
                                'status': 'completed',
                                'lentochka_status_path': str(lentochka_status_path),
                                'subdirs': [d.name for d in rsync_dir.iterdir() if d.is_dir()]
                            }
                            stanzas.append(stanza)
                            self.lentochka_log.log_lentochka_info(
                                f"Stanza added to processing queue: {repo_path} (at {rsync_status_path})")
                        else:
                            stanza_name = rsync_dir.name
                            self.lentochka_log.log_lentochka_error(
                                f"MISSING STATUS (SKIPPED IN QUEUE): Repo '{repo_path}', stanza '{stanza_name}': "
                                f"rsync.status at {rsync_status_path} has status '{status_content}' "
                                f"(not 'complete'). Skipping this stanza. "
                                f"Action: Wait for rsync to complete or check file manually."
                            )
                except IOError as exception:
                    stanza_name = rsync_dir.name
                    self.lentochka_log.log_lentochka_error(
                        f"MISSING STATUS (READ ERROR IN QUEUE): Repo '{repo_path}', stanza '{stanza_name}': "
                        f"Failed to read rsync.status at {rsync_status_path}. "
                        f"Error: {exception}. Skipping this stanza."
                    )
        return stanzas
    def run_dsmc_command(self, stanza_info: Dict[str, Any], start_time: datetime.datetime) -> int:

        command_str = stanza_info.get('dsmc_command', '')
        if not command_str:
            self.lentochka_log.log_dsmc_error("DSMC command is empty. Skipping execution.")
            return 1
        self.lentochka_log.log_dsmc_info(f"Executing DSMC command: {command_str}")
        try:
            process = subprocess.Popen(
                command_str,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()
            if stdout:
                for line in stdout.splitlines():
                    self.lentochka_log.dsmc_logger.info(line)
            if stderr:
                for line in stderr.splitlines():
                    self.lentochka_log.dsmc_logger.error(line)
            self.lentochka_log.log_dsmc_info(f"DSMC command finished with code {process.returncode}")
            return process.returncode
        except Exception as e:
            self.lentochka_log.log_dsmc_error(f"Error executing DSMC command: {e}")
            return 1
        def process_stanza(self, stanza_info: Dict[str, Any]) -> bool:
        try:
            self.lentochka_log.validate_dsmc_log_dir()
            start_time = datetime.datetime.now()
            self.lentochka_log.log_lentochka_info(
                f"Starting to process stanza: {stanza_info['repo_path']} at {start_time} "
                f"(backup: {stanza_info['backup_path']})"
            )

            backup_path = Path(stanza_info['backup_path'])
            archive_path = Path(stanza_info.get('archive_path', '')) if stanza_info.get('archive_path') else None
            lentochka_status_path = Path(stanza_info['lentochka_status_path'])

            if lentochka_status_path.exists():
                self.lentochka_log.log_lentochka_info(
                    f"Stanza already processed, skipping: {stanza_info['repo_path']}"
                )
                return True

            if not backup_path.exists():
                self.lentochka_log.log_lentochka_error(f"Backup path missing: {backup_path}")
                return False

            if stanza_info.get('status') == 'failed':
                self.lentochka_log.log_lentochka_info(f"Stanza marked as failed, skipping: {stanza_info['repo_path']}")
                return False

            dsmc_path = self.config.get('DSMC', 'dsmc_path', fallback='dsmc')
            dsmc_command_template = self.config.get(
                'DSMC',
                'dsmc_command_template',
                fallback='{dsmc_path} incremental "{backup_dirs}" -su=yes -se=ROV1-TSM -subdir=yes'
            )

            cmd_backup = dsmc_command_template.format(
                dsmc_path=dsmc_path,
                backup_dirs=str(backup_path) + "/"
            )
            self.lentochka_log.log_dsmc_info(f"DSMC BACKUP command: {cmd_backup}")
            rc1 = self.run_dsmc_command({**stanza_info, 'dsmc_command': cmd_backup}, start_time)

            rc2 = 0
            if archive_path and archive_path.exists():
                cmd_archive = dsmc_command_template.format(
                    dsmc_path=dsmc_path,
                    backup_dirs=str(archive_path) + "/"
                )
                self.lentochka_log.log_dsmc_info(f"DSMC ARCHIVE command: {cmd_archive}")
                rc2 = self.run_dsmc_command({**stanza_info, 'dsmc_command': cmd_archive}, start_time)

            backed_up = 0
            log_path = self.lentochka_log.current_dsmc_session_log_file
            if os.path.exists(log_path):
                log_text = Path(log_path).read_text(errors='ignore')
                for line in log_text.splitlines():
                    if "Общее число объектов, для которых созданы резервные копии:" in line:
                        try:
                            backed_up += int(line.split()[-1].replace(',', ''))
                        except:
                            pass
                    if "Total number of objects backed up:" in line:
                        try:
                            backed_up += int(line.split(':')[-1].strip())
                        except:
                            pass

            self.lentochka_log.log_dsmc_info(f"DSMC reported backed up objects: {backed_up}")

            if backed_up > 0:
                end_time = datetime.datetime.now()
                status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}\nObjects: {backed_up}"
                try:
                    with open(lentochka_status_path, 'w') as f:
                        f.write(status_content)
                    self.lentochka_log.log_lentochka_info(
                        f"STANZA SUCCESS: {stanza_info['repo_path']} → {backed_up} objects backed up"
                    )
                    return True
                except Exception as e:
                    self.lentochka_log.log_lentochka_error(f"Failed to write lentochka-status: {e}")
                    return False
            else:
                self.lentochka_log.log_lentochka_error(
                    f"ZERO objects backed up (rc_backup={rc1}, rc_archive={rc2}) — stanza NOT marked as done: {stanza_info['repo_path']}"
                )
                return False

        except Exception as e:
            self.lentochka_log.log_lentochka_error(f"Uncaught exception in process_stanza: {e}")
            return False


def main():
    global monitoring
    dsmc_log = None  
    monitoring = None
    try:
        dsmc_log = DsmcPlusLentochkaLogs()
        monitoring = MonitoringHandler(dsmc_log.config, dsmc_log.log_manager)
        max_instances = dsmc_log.config.getint('Process', 'max_instances', fallback=1)
        lock_file = dsmc_log.config.get('Paths', 'lock_file', fallback='/tmp/lentochka_dsmc.lock')
        process_locker = ProcessLocker(lock_file, dsmc_log.log_manager, max_instances)
        pid_dir = '/tmp'
        for pid_file in glob.glob(os.path.join(pid_dir, 'dsmc_*.pid')):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                os.kill(pid, 0)  
                os.kill(pid, signal.SIGTERM)  
                dsmc_log.log_manager.info(f"Found old DSMC process with PID {pid}, terminated.")
                os.remove(pid_file)
            except (OSError, ValueError, IOError):
                dsmc_log.log_manager.info(f"Old PID file {pid_file} is stale or invalid, removed.")
                try:
                    os.remove(pid_file)
                except OSError:
                    pass
        with process_locker:
            if monitoring.script and not os.path.exists(monitoring.script):
                dsmc_log.log_manager.error(f"Monitoring script not found at path: {monitoring.script}")
                monitoring.enabled = False
            dsmc_log.log_manager.info("Starting main script execution.")
            stanza_processor = StanzaProcessor(dsmc_log.config, dsmc_log)
            stanzas = stanza_processor.find_stanzas()
            dsmc_path = dsmc_log.config.get('DSMC', 'dsmc_path', fallback='dsmc')
            dsmc_exists = shutil.which(dsmc_path) is not None
            if not dsmc_exists:
                error_msg = "DSMC utility not found. Specify the correct path in LentochkaDSMC.ini"
                dsmc_log.log_manager.error(error_msg)
                if monitoring and monitoring.enabled:
                    try:
                        monitoring.send_metric("dsmc_not_found", 1, "ERROR")
                    except Exception as send_error:
                        dsmc_log.log_manager.error(f"Error sending metric for dsmc not found: {send_error}")
                sys.exit(1)
            
            successful_copies = 0
            failed_copies = 0
            for stanza in stanzas:
                dsmc_log.log_manager.info(f"Processing stanza: {stanza['repo_path']}...")
                if stanza_processor.process_stanza(stanza):
                    successful_copies += 1
                    if monitoring.enabled:
                        monitoring.send_metric("processed_stanzas", 1)
                else:
                    failed_copies += 1
                    if monitoring.enabled:
                        monitoring.send_metric("failed_stanzas", 1)
            dsmc_log.log_manager.info(
                f"Results: Processed {len(stanzas)} stanzas, "
                f"successfully copied: {successful_copies}, errors: {failed_copies}"
            )
            dsmc_log.cleanup_empty_logs()
            dsmc_log.close_iteration_log()

            if dsmc_log.lentochka_status_dir and os.path.exists(dsmc_log.lentochka_status_dir):
                try:
                    if not os.listdir(dsmc_log.lentochka_status_dir):
                        os.rmdir(dsmc_log.lentochka_status_dir)
                        dsmc_log.log_manager.info(
                            f"Removed empty lentochka_status_dir: {dsmc_log.lentochka_status_dir}")
                except Exception as e:
                    dsmc_log.log_manager.error(f"Error removing empty lentochka_status_dir: {e}")

            if monitoring.log_cleanup_enabled:
                log_dir = dsmc_log.config.get('Paths', 'log_dir')
                if not log_dir:
                    dsmc_log.log_manager.warning("No log_dir specified, skipping cleanup.")
                else:
                    monitoring.cleanup_logs(log_dir, monitoring.log_retention_days)
            dsmc_log.log_manager.info("Script completed successfully.")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"File not found: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for FileNotFoundError: {send_error}")
        sys.exit(1)
    except ValueError as e:
        print(f"Value error: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"Value error: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for ValueError: {send_error}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"Unexpected error: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for unexpected error: {send_error}")
        sys.exit(1)
if __name__ == '__main__':
    main()
