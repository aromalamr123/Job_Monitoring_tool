import csv
import logging
import os
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path


LOGGER = logging.getLogger(__name__)

SCHEDULE_COLUMNS = [
    "schedule_id",
    "job_id",
    "job_name",
    "run_at",
    "recurrence",
    "status",
    "duration_seconds",
    "created_at",
    "last_run_at",
    "execution_id",
]
EXECUTION_COLUMNS = [
    "execution_id",
    "schedule_id",
    "job_id",
    "job_name",
    "started_at",
    "ended_at",
    "status",
    "progress",
]
RECURRENCES = {"Once", "Daily"}


def _timestamp(value):
    return value.astimezone().isoformat(timespec="seconds")


class SchedulerService:
    """Local CSV-backed scheduler for simulated demonstration jobs."""

    def __init__(self, data_dir, poll_interval=1):
        self.data_dir = Path(data_dir)
        self.schedules_file = self.data_dir / "job_schedules.csv"
        self.executions_file = self.data_dir / "job_executions.csv"
        self.logs_dir = self.data_dir / "job_scheduler_logs"
        self.poll_interval = poll_interval
        self._lock = threading.RLock()
        self._active_executions = set()
        self._started = False
        self._stop_event = threading.Event()
        self._ensure_files()

    def _ensure_files(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        for path, columns in (
            (self.schedules_file, SCHEDULE_COLUMNS),
            (self.executions_file, EXECUTION_COLUMNS),
        ):
            if not path.exists():
                self._write_rows(path, columns, [])

    @staticmethod
    def _read_rows(path, columns):
        with path.open("r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames != columns:
                raise ValueError(f"Unexpected columns in {path.name}.")
            return list(reader)

    @staticmethod
    def _write_rows(path, columns, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                newline="",
                encoding="utf-8",
                dir=path.parent,
                delete=False,
            ) as file:
                temp_path = Path(file.name)
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            os.replace(temp_path, path)
        except Exception:
            if temp_path and temp_path.exists():
                temp_path.unlink()
            raise

    def list_schedules(self):
        with self._lock:
            return self._read_rows(self.schedules_file, SCHEDULE_COLUMNS)

    def list_executions(self):
        with self._lock:
            return self._read_rows(self.executions_file, EXECUTION_COLUMNS)

    def create_schedule(self, job_id, job_name, run_at, recurrence, duration_seconds):
        if recurrence not in RECURRENCES:
            raise ValueError(f"Recurrence must be one of {sorted(RECURRENCES)}.")
        if run_at.tzinfo is None:
            raise ValueError("Scheduled time must include a timezone.")
        if run_at <= datetime.now().astimezone():
            raise ValueError("Scheduled time must be in the future.")
        if not 1 <= int(duration_seconds) <= 120:
            raise ValueError("Demo duration must be between 1 and 120 seconds.")

        schedule = {
            "schedule_id": uuid.uuid4().hex,
            "job_id": str(job_id),
            "job_name": str(job_name),
            "run_at": _timestamp(run_at),
            "recurrence": recurrence,
            "status": "SCHEDULED",
            "duration_seconds": str(int(duration_seconds)),
            "created_at": _timestamp(datetime.now().astimezone()),
            "last_run_at": "",
            "execution_id": "",
        }
        with self._lock:
            rows = self._read_rows(self.schedules_file, SCHEDULE_COLUMNS)
            rows.append(schedule)
            self._write_rows(self.schedules_file, SCHEDULE_COLUMNS, rows)
        return schedule["schedule_id"]

    def cancel_schedule(self, schedule_id):
        with self._lock:
            rows = self._read_rows(self.schedules_file, SCHEDULE_COLUMNS)
            found = False
            for row in rows:
                if row["schedule_id"] == schedule_id and row["status"] == "SCHEDULED":
                    row["status"] = "CANCELLED"
                    found = True
                    break
            if found:
                self._write_rows(self.schedules_file, SCHEDULE_COLUMNS, rows)
            return found

    def start(self):
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread = threading.Thread(
                target=self._scheduler_loop,
                name="local-job-scheduler",
                daemon=True,
            )
            self._thread.start()

    def _scheduler_loop(self):
        while not self._stop_event.is_set():
            self._start_due_schedules()
            self._stop_event.wait(self.poll_interval)

    def _start_due_schedules(self):
        now = datetime.now().astimezone()
        due = []
        with self._lock:
            schedules = self._read_rows(self.schedules_file, SCHEDULE_COLUMNS)
            executions = self._read_rows(self.executions_file, EXECUTION_COLUMNS)
            changed = False
            for schedule in schedules:
                if schedule["status"] != "SCHEDULED":
                    continue
                if datetime.fromisoformat(schedule["run_at"]) > now:
                    continue

                execution_id = uuid.uuid4().hex
                schedule["status"] = "RUNNING"
                schedule["last_run_at"] = _timestamp(now)
                schedule["execution_id"] = execution_id
                executions.append(
                    {
                        "execution_id": execution_id,
                        "schedule_id": schedule["schedule_id"],
                        "job_id": schedule["job_id"],
                        "job_name": schedule["job_name"],
                        "started_at": _timestamp(now),
                        "ended_at": "",
                        "status": "RUNNING",
                        "progress": "0",
                    }
                )
                due.append((schedule.copy(), execution_id))
                changed = True
            if changed:
                self._write_rows(self.schedules_file, SCHEDULE_COLUMNS, schedules)
                self._write_rows(self.executions_file, EXECUTION_COLUMNS, executions)

        for schedule, execution_id in due:
            with self._lock:
                self._active_executions.add(execution_id)
            thread = threading.Thread(
                target=self._execute_demo_job,
                args=(schedule, execution_id),
                name=f"demo-job-{execution_id[:8]}",
                daemon=True,
            )
            thread.start()

    def _append_log(self, execution_id, message):
        log_file = self.logs_dir / f"{execution_id}.txt"
        with log_file.open("a", encoding="utf-8") as file:
            file.write(f"[{_timestamp(datetime.now().astimezone())}] {message}\n")
            file.flush()

    def _update_execution(self, execution_id, status, progress, ended_at=""):
        with self._lock:
            rows = self._read_rows(self.executions_file, EXECUTION_COLUMNS)
            for row in rows:
                if row["execution_id"] == execution_id:
                    row["status"] = status
                    row["progress"] = str(progress)
                    row["ended_at"] = ended_at
                    break
            self._write_rows(self.executions_file, EXECUTION_COLUMNS, rows)

    def _finish_schedule(self, schedule, execution_id, status):
        with self._lock:
            rows = self._read_rows(self.schedules_file, SCHEDULE_COLUMNS)
            for row in rows:
                if row["schedule_id"] != schedule["schedule_id"]:
                    continue
                if row["recurrence"] == "Daily" and status == "COMPLETED":
                    next_run = datetime.fromisoformat(row["run_at"]) + timedelta(days=1)
                    while next_run <= datetime.now().astimezone():
                        next_run += timedelta(days=1)
                    row["run_at"] = _timestamp(next_run)
                    row["status"] = "SCHEDULED"
                else:
                    row["status"] = status
                row["execution_id"] = execution_id
                break
            self._write_rows(self.schedules_file, SCHEDULE_COLUMNS, rows)

    def _execute_demo_job(self, schedule, execution_id):
        duration = int(schedule["duration_seconds"])
        log_file = self.logs_dir / f"{execution_id}.txt"
        try:
            self._append_log(
                execution_id,
                f"Starting demo execution for {schedule['job_name']} ({schedule['job_id']}).",
            )
            self._append_log(
                execution_id,
                "This is a simulated run; no external job command is executed.",
            )
            steps = 5
            for step in range(1, steps + 1):
                time.sleep(duration / steps)
                progress = step * 20
                self._append_log(execution_id, f"Demo work in progress: {progress}%.")
                self._update_execution(execution_id, "RUNNING", progress)

            ended_at = _timestamp(datetime.now().astimezone())
            self._append_log(execution_id, "Demo execution completed successfully.")
            self._update_execution(execution_id, "COMPLETED", 100, ended_at)
            self._finish_schedule(schedule, execution_id, "COMPLETED")
        except Exception as error:
            LOGGER.exception("Demo execution %s failed", execution_id)
            self._append_log(execution_id, f"Execution failed: {error}")
            self._update_execution(
                execution_id,
                "FAILED",
                0,
                _timestamp(datetime.now().astimezone()),
            )
            self._finish_schedule(schedule, execution_id, "FAILED")
        finally:
            with self._lock:
                self._active_executions.discard(execution_id)

    def read_log(self, execution_id):
        if not execution_id or Path(execution_id).name != execution_id:
            raise ValueError("Invalid execution ID.")
        log_file = self.logs_dir / f"{execution_id}.txt"
        if not log_file.exists():
            return ""
        return log_file.read_text(encoding="utf-8")
