# Batch Job Dashboard — 30-job synthetic data pack

## Contents
- `data/job_config.csv` — 30 jobs
- `data/execution_history.csv` — 180 execution records
- `data/dependencies.csv` — 36 dependency links
- `docs/data_schema.md` — column definitions

All data is fictional and is not Wipro production data.

## Update your existing project
1. Stop any running Python script or Streamlit app.
2. Open your existing `MTech_Batch_Dashboard` folder in VS Code.
3. Back up the current `data` folder by copying it and naming the copy `data_backup`.
4. Extract this ZIP to a temporary location.
5. Open the extracted `batch_job_30_dataset` folder. Copy its `data` folder into your existing project, replacing the old `data` folder/files when prompted.
6. Copy `docs/data_schema.md` into your project's `docs` folder.
7. In the VS Code terminal, activate `.venv` if needed. Then run `python validate_data.py`.
8. Expected counts: 30 jobs, 180 execution records, 36 dependency links. An unfinished-job warning is expected because queued/running records have blank end times.

Do not replace `.venv`, `requirements.txt`, `check_data.py`, or `validate_data.py`; this pack does not include them.

## Job Scheduler demo

Run the dashboard with `streamlit run app.py` and open **Job Scheduler** from
the navigation menu. Select a configured job, choose a one-time or daily
schedule, and set a short simulated run duration. The page refreshes execution
status and the selected run's log while the app is running.

This is a local college-project demo: scheduled runs simulate progress and do
not execute real external job commands. Schedule definitions are stored in
`data/job_schedules.csv`, run status in `data/job_executions.csv`, and each
execution's text log in `data/job_scheduler_logs/`. Keep the Streamlit process
running for scheduled jobs to be picked up. The local scheduler is intended
for a single app process and does not provide user accounts or multi-process
coordination.
