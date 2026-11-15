import time
from datetime import datetime, timedelta

from scheduler import SchedulerService


def test_schedule_is_persisted_and_can_be_cancelled(tmp_path):
    service = SchedulerService(tmp_path)
    run_at = datetime.now().astimezone() + timedelta(minutes=5)

    schedule_id = service.create_schedule(
        "JOB001", "Sample job", run_at, "Once", 5
    )

    schedules = service.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_id"] == schedule_id
    assert schedules[0]["status"] == "SCHEDULED"
    assert service.cancel_schedule(schedule_id)
    assert service.list_schedules()[0]["status"] == "CANCELLED"
    assert not service.cancel_schedule(schedule_id)


def test_due_job_records_progress_and_writes_local_log(tmp_path):
    service = SchedulerService(tmp_path)
    run_at = datetime.now().astimezone() + timedelta(seconds=1)
    schedule_id = service.create_schedule(
        "JOB001", "Sample job", run_at, "Once", 1
    )

    time.sleep(1.05)
    service._start_due_schedules()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        executions = service.list_executions()
        if executions and executions[0]["status"] == "COMPLETED":
            break
        time.sleep(0.05)

    assert len(executions) == 1
    assert executions[0]["schedule_id"] == schedule_id
    assert executions[0]["status"] == "COMPLETED"
    assert executions[0]["progress"] == "100"
    log = service.read_log(executions[0]["execution_id"])
    assert "simulated run" in log
    assert "completed successfully" in log
    assert service.list_schedules()[0]["status"] == "COMPLETED"


def test_daily_schedule_is_rescheduled_after_success(tmp_path):
    service = SchedulerService(tmp_path)
    run_at = datetime.now().astimezone() + timedelta(seconds=1)
    schedule_id = service.create_schedule(
        "JOB001", "Sample job", run_at, "Daily", 1
    )

    time.sleep(1.05)
    service._start_due_schedules()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        schedule = service.list_schedules()[0]
        executions = service.list_executions()
        if executions and schedule["status"] == "SCHEDULED":
            break
        time.sleep(0.05)

    assert len(executions) == 1
    assert schedule["schedule_id"] == schedule_id
    assert schedule["status"] == "SCHEDULED"
    assert datetime.fromisoformat(schedule["run_at"]) > datetime.now().astimezone()
