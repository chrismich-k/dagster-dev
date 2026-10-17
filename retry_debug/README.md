# RETRY-DEBUG diagnostic build (dagster 1.13.23+retrydebug1)

Diagnostic-only branch. **Not for upstream.** Behavior is unchanged; the run retry daemon
(`EventLogConsumerDaemon`) additionally emits engine events prefixed with `[RETRY-DEBUG]` on the
affected run (visible in the run's event log in the UI) and warnings in the daemon log.

Suspected cause: `should_retry()` sees a run whose RUN_FAILURE event is already stored, but whose
status is not yet FAILURE and which has no `dagster/will_retry` tag (the run worker writes event,
status and tag in three separate steps). It returns False and the event cursor moves past the run,
so the run is never retried. In that case the daemon reports
`[RETRY-DEBUG] should_retry=False: tag dagster/will_retry is missing ...` and, after a re-check
(`DAGSTER_RETRY_DEBUG_RECHECK_DELAY_SECONDS`, default 2), `[RETRY-DEBUG] RACE CONFIRMED ...`.

## Build & install (daemon/webserver venv only)

```bash
uv build --wheel python_modules/dagster -o dist
uv pip install --python /mnt/myproject/dagster/venv/bin/python --no-deps --reinstall \
    dist/dagster-1.13.23+retrydebug1-py3-none-any.whl
uv pip list --python /mnt/myproject/dagster/venv/bin/python | grep '^dagster '   # 1.13.23+retrydebug1
sudo systemctl restart dagster-daemon dagster-webserver
```

Code location / run worker images stay unchanged. Do not `pip install -U` the venv during the
test week (it would replace the debug build with PyPI dagster).

Rollback: `uv pip install --python /mnt/myproject/dagster/venv/bin/python dagster==1.13.23`
and restart the services.

## Evaluate

```bash
journalctl -u dagster-daemon | grep RETRY-DEBUG
```
```sql
SELECT run_id, timestamp, event FROM event_logs
WHERE event LIKE '%[RETRY-DEBUG]%' ORDER BY timestamp;
```

## Local repro (simulated race)

```bash
python retry_debug/repro_race.py   # prints RESULT: OK
```
