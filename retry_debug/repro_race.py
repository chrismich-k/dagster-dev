"""Simulate the suspected race between the run worker and the EventLogConsumerDaemon.

Scenario A (race): RUN_FAILURE event is stored, but run status / will_retry tag are written only
later (while the daemon is already deciding). Expected: [RETRY-DEBUG] events incl. RACE CONFIRMED,
and NO retry run, even after further daemon ticks.

Scenario B (control): normal failure via instance.handle_new_event. Expected: retry run is created,
no "missing tag" debug event.
"""

import os
import sys
import threading
import time

from dagster import DagsterEventType
from dagster._core.events import DagsterEvent, JobFailureData
from dagster._core.events.log import EventLogEntry
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import RETRY_STRATEGY_TAG, WILL_RETRY_TAG
from dagster._core.test_utils import (
    create_run_for_test,
    create_test_daemon_workspace_context,
    instance_for_test,
)
from dagster._core.workspace.load_target import PythonFileTarget
from dagster._daemon.auto_run_reexecution.event_log_consumer import EventLogConsumerDaemon

HERE = os.path.dirname(os.path.abspath(__file__))


def failure_entry(run):
    dagster_event = DagsterEvent(
        event_type_value=DagsterEventType.RUN_FAILURE.value,
        job_name=run.job_name,
        message="simulated run failure",
        event_specific_data=JobFailureData(error=None, failure_reason=None),
    )
    return EventLogEntry(
        error_info=None,
        level="ERROR",
        user_message="",
        run_id=run.run_id,
        timestamp=time.time(),
        dagster_event=dagster_event,
    )


def tick(daemon, ctx):
    list(daemon.run_iteration(ctx))


def debug_messages(instance, run_id):
    return [
        r.event_log_entry.message
        for r in instance.get_records_for_run(run_id).records
        if "[RETRY-DEBUG]" in (r.event_log_entry.message or "")
    ]


def run_group_ids(instance, run_id):
    return [r.run_id for r in instance.get_run_group(run_id)[1]]


def main():
    ok = True
    with instance_for_test(
        overrides={
            "run_retries": {"enabled": True, "max_retries": 2},
            "run_coordinator": {
                "module": "dagster._core.test_utils",
                "class": "MockedRunCoordinator",
            },
        }
    ) as instance:
        target = PythonFileTarget(
            python_file=os.path.join(HERE, "defs.py"),
            attribute=None,
            working_directory=HERE,
            location_name="test_loc",
        )
        with create_test_daemon_workspace_context(target, instance) as ctx:
            remote_job = (
                ctx.create_request_context()
                .get_code_location("test_loc")
                .get_repository("__repository__")
                .get_full_job("fail_job")
            )

            def new_run():
                return create_run_for_test(
                    instance,
                    job_name="fail_job",
                    status=DagsterRunStatus.STARTED,
                    tags={RETRY_STRATEGY_TAG: "ALL_STEPS"},
                    remote_job_origin=remote_job.get_remote_origin(),
                    job_code_origin=remote_job.get_python_origin(),
                )

            daemon = EventLogConsumerDaemon()
            tick(daemon, ctx)  # initialize cursors

            # ---------------- Scenario A: race ----------------
            run_a = new_run()
            entry = failure_entry(run_a)
            instance.event_log_storage.store_event(entry)  # step 1 only

            def finish_worker_steps():
                time.sleep(0.5)  # daemon is now inside should_retry / recheck delay
                instance.handle_run_event(run_a.run_id, entry.get_dagster_event())  # step 2
                instance.add_run_tags(run_a.run_id, {WILL_RETRY_TAG: "true"})  # step 3

            t = threading.Thread(target=finish_worker_steps)
            t.start()
            tick(daemon, ctx)
            t.join()
            tick(daemon, ctx)
            tick(daemon, ctx)

            msgs_a = debug_messages(instance, run_a.run_id)
            group_a = run_group_ids(instance, run_a.run_id)
            status_a = instance.get_run_by_id(run_a.run_id).status
            print("\n=== Scenario A (race) ===")
            print("final status:", status_a.value, "| tags will_retry:",
                  instance.get_run_by_id(run_a.run_id).tags.get(WILL_RETRY_TAG))
            print("runs in group:", len(group_a))
            for m in msgs_a:
                print("  ", m)
            ok &= status_a == DagsterRunStatus.FAILURE
            ok &= len(group_a) == 1  # NOT retried
            ok &= any("should_retry=False" in m for m in msgs_a)
            ok &= any("RACE CONFIRMED" in m for m in msgs_a)

            # ---------------- Scenario B: control ----------------
            run_b = new_run()
            instance.handle_new_event(failure_entry(run_b))  # steps 1-3 at once
            tick(daemon, ctx)
            msgs_b = debug_messages(instance, run_b.run_id)
            group_b = run_group_ids(instance, run_b.run_id)
            print("\n=== Scenario B (control) ===")
            print("final status:", instance.get_run_by_id(run_b.run_id).status.value,
                  "| tags will_retry:", instance.get_run_by_id(run_b.run_id).tags.get(WILL_RETRY_TAG))
            print("runs in group:", len(group_b))
            for m in msgs_b:
                print("  ", m)
            ok &= len(group_b) == 2  # retried
            ok &= not msgs_b

    print("\nRESULT:", "OK" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
