import logging
import os
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import dagster._check as check
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.definitions.selector import JobSubsetSelector
from dagster._core.errors import DagsterRunNotFoundError
from dagster._core.events import DagsterEventType, EngineEventData, RunFailureReason
from dagster._core.execution.backfill import BULK_ACTION_TERMINAL_STATUSES
from dagster._core.execution.plan.resume_retry import ReexecutionStrategy
from dagster._core.execution.retries import auto_reexecution_should_retry_run
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunRecord
from dagster._core.storage.tags import (
    AUTO_RETRY_RUN_ID_TAG,
    BACKFILL_ID_TAG,
    MAX_RETRIES_TAG,
    RETRY_NUMBER_TAG,
    RETRY_ON_ASSET_OR_OP_FAILURE_TAG,
    RETRY_STRATEGY_TAG,
    RUN_FAILURE_REASON_TAG,
    WILL_RETRY_TAG,
)
from dagster._core.workspace.context import IWorkspaceProcessContext
from dagster._daemon.utils import DaemonErrorCapture
from dagster._utils.tags import get_boolean_tag_value

DEFAULT_REEXECUTION_POLICY = ReexecutionStrategy.FROM_FAILURE

# ---------------------------------------------------------------------------------------------
# RETRY-DEBUG instrumentation (diagnostic build only, not intended for upstream).
# Emits engine events prefixed with RETRY_DEBUG_PREFIX on the affected run so that it is visible
# in the run's event log in the UI, plus a warning in the daemon log. Behavior is unchanged.
# ---------------------------------------------------------------------------------------------
RETRY_DEBUG_PREFIX = "[RETRY-DEBUG]"
_RETRY_DEBUG_RECHECK_DELAY_SECONDS = float(
    os.environ.get("DAGSTER_RETRY_DEBUG_RECHECK_DELAY_SECONDS", "2")
)
_retry_debug_fallback_logger = logging.getLogger("dagster.daemon.RetryDebug")


def _retry_debug_run_metadata(run: DagsterRun, instance: DagsterInstance) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "debug_checked_at": MetadataValue.text(time.strftime("%Y-%m-%dT%H:%M:%S%z")),
        "debug_checked_at_ts": MetadataValue.float(time.time()),
        "run_status_seen_by_daemon": MetadataValue.text(run.status.value),
    }
    for tag in (
        WILL_RETRY_TAG,
        RUN_FAILURE_REASON_TAG,
        MAX_RETRIES_TAG,
        RETRY_NUMBER_TAG,
        RETRY_ON_ASSET_OR_OP_FAILURE_TAG,
        AUTO_RETRY_RUN_ID_TAG,
        BACKFILL_ID_TAG,
    ):
        metadata[f"tag {tag}"] = MetadataValue.text(str(run.tags.get(tag)))
    try:
        metadata["instance run_retries.enabled"] = MetadataValue.text(
            str(instance.run_retries_enabled)
        )
        metadata["instance run_retries.max_retries"] = MetadataValue.text(
            str(instance.run_retries_max_retries)
        )
        metadata["instance run_retries.retry_on_asset_or_op_failure"] = MetadataValue.text(
            str(instance.run_retries_retry_on_asset_or_op_failure)
        )
        failure_records = instance.get_records_for_run(
            run.run_id, of_type=DagsterEventType.RUN_FAILURE, limit=1, ascending=False
        ).records
        if failure_records:
            metadata["run_failure_event storage_id"] = MetadataValue.int(
                failure_records[0].storage_id
            )
            metadata["run_failure_event timestamp"] = MetadataValue.float(
                failure_records[0].timestamp
            )
        else:
            metadata["run_failure_event storage_id"] = MetadataValue.text("none found")
        run_record = instance.get_run_record_by_id(run.run_id)
        if run_record:
            metadata["run_record status (now)"] = MetadataValue.text(
                run_record.dagster_run.status.value
            )
            metadata["run_record update_timestamp (now)"] = MetadataValue.text(
                str(run_record.update_timestamp)
            )
            metadata["run_record end_time (now)"] = MetadataValue.text(str(run_record.end_time))
    except Exception as e:
        metadata["debug_metadata_error"] = MetadataValue.text(repr(e))
    return metadata


def _report_retry_debug(
    instance: DagsterInstance,
    run: DagsterRun,
    message: str,
    logger: logging.Logger | None,
    extra_metadata: Mapping[str, Any] | None = None,
) -> None:
    """Report a RETRY-DEBUG engine event on the run. Never raises."""
    log = logger or _retry_debug_fallback_logger
    full_message = f"{RETRY_DEBUG_PREFIX} {message}"
    try:
        metadata = _retry_debug_run_metadata(run, instance)
        metadata.update(extra_metadata or {})
        log.warning(f"{full_message} (run_id={run.run_id})")
        instance.report_engine_event(full_message, run, engine_event_data=EngineEventData(metadata))
    except Exception:
        log.exception(f"{RETRY_DEBUG_PREFIX} failed to report debug event for run {run.run_id}")


def _retry_debug_missing_tag_not_failed(
    run: DagsterRun, instance: DagsterInstance, logger: logging.Logger | None
) -> None:
    """Instrumentation for the suspected race: the daemon saw the RUN_FAILURE event, but the run
    record is not (yet) FAILURE and has no WILL_RETRY_TAG. The event log consumer cursor will move
    past the event, so the run will never be looked at again.
    """
    if run.status == DagsterRunStatus.SUCCESS:
        # expected: run was picked up because of its RUN_SUCCESS event
        return

    _report_retry_debug(
        instance,
        run,
        f"should_retry=False: tag {WILL_RETRY_TAG} is missing and run status seen by the daemon"
        f" is {run.status.value} (not FAILURE). The event log consumer cursor will advance past"
        " this run's events.",
        logger,
    )

    # Re-check after a short delay to prove the race: if the run is FAILURE now (or got the
    # WILL_RETRY_TAG), the daemon decided on stale state and will not look at this run again.
    try:
        if _RETRY_DEBUG_RECHECK_DELAY_SECONDS > 0:
            time.sleep(_RETRY_DEBUG_RECHECK_DELAY_SECONDS)
        rechecked_run = instance.get_run_by_id(run.run_id)
    except Exception as e:
        _report_retry_debug(instance, run, f"recheck failed: {e!r}", logger)
        return
    if rechecked_run is None:
        return

    rechecked_will_retry = rechecked_run.tags.get(WILL_RETRY_TAG)
    recheck_metadata = {
        "recheck delay seconds": MetadataValue.float(_RETRY_DEBUG_RECHECK_DELAY_SECONDS),
        "status before recheck": MetadataValue.text(run.status.value),
        "status after recheck": MetadataValue.text(rechecked_run.status.value),
        f"tag {WILL_RETRY_TAG} after recheck": MetadataValue.text(str(rechecked_will_retry)),
    }
    if rechecked_run.status == DagsterRunStatus.FAILURE or rechecked_will_retry is not None:
        _report_retry_debug(
            instance,
            rechecked_run,
            f"RACE CONFIRMED: run status changed {run.status.value} -> "
            f"{rechecked_run.status.value} ({WILL_RETRY_TAG}={rechecked_will_retry}) after the"
            " daemon already decided not to retry. The event log consumer cursor has moved past"
            " the RUN_FAILURE event, so this run will NOT be retried automatically.",
            logger,
            recheck_metadata,
        )
    else:
        _report_retry_debug(
            instance,
            rechecked_run,
            f"recheck: run status still {rechecked_run.status.value} after"
            f" {_RETRY_DEBUG_RECHECK_DELAY_SECONDS}s, {WILL_RETRY_TAG}={rechecked_will_retry}.",
            logger,
            recheck_metadata,
        )


def should_retry(
    run: DagsterRun, instance: DagsterInstance, logger: logging.Logger | None = None
) -> bool:
    """A more robust method of determining is a run should be retried by the daemon than just looking
    at the WILL_RETRY_TAG. We account for the case where the code version is old and doesn't set the
    WILL_RETRY_TAG. If the tag wasn't set for a run failure, we set it so that other daemons can use the
    WILL_RETRY_TAG to determine if the run should be retried.
    """
    will_retry_tag_value = run.tags.get(WILL_RETRY_TAG)
    run_failure_reason = (
        RunFailureReason(run.tags.get(RUN_FAILURE_REASON_TAG))
        if run.tags.get(RUN_FAILURE_REASON_TAG)
        else None
    )
    if will_retry_tag_value is None:
        # If the run doesn't have the WILL_RETRY_TAG, and the run is failed, we
        # recalculate if the run should be retried to ensure backward compatibilty
        if run.status == DagsterRunStatus.FAILURE:
            should_retry_run = auto_reexecution_should_retry_run(instance, run, run_failure_reason)
            # add the tag to the run so that it can be used in other parts of the system
            instance.add_run_tags(run.run_id, {WILL_RETRY_TAG: str(should_retry_run).lower()})
        else:
            # run is not failed, and shouldn't be retried
            # possibly discards runs that failed but didn't get the DagsterRunStatus.FAILURE yet!
            # RETRY-DEBUG: check and report as an event
            _retry_debug_missing_tag_not_failed(run, instance, logger)
            return False
    else:
        should_retry_run = get_boolean_tag_value(will_retry_tag_value, default_value=False)

    if should_retry_run:
        # If the run is part of a backfill that has reached a terminal state (canceled,
        # failed, completed) or has been deleted, do not auto-retry. Manual retries are
        # unaffected since they bypass this function entirely.
        backfill_id = run.tags.get(BACKFILL_ID_TAG)
        if backfill_id is not None:
            backfill = instance.get_backfill(backfill_id)
            if backfill is None:
                instance.report_engine_event(
                    "Not retrying run since it is part of a backfill that no longer exists.",
                    run,
                )
                return False
            if backfill.status in BULK_ACTION_TERMINAL_STATUSES:
                instance.report_engine_event(
                    "Not retrying run since it is part of a backfill that is in a terminal"
                    f" state ({backfill.status.value}).",
                    run,
                )
                return False
        return should_retry_run
    else:
        # one of the reasons we may not retry a run is if it is a step failure and system is
        # set to not retry on op/asset failures. In this case, we log
        # an engine event
        retry_on_asset_or_op_failure = get_boolean_tag_value(
            run.tags.get(RETRY_ON_ASSET_OR_OP_FAILURE_TAG),
            default_value=instance.run_retries_retry_on_asset_or_op_failure,
        )
        if run_failure_reason == RunFailureReason.STEP_FAILURE and not retry_on_asset_or_op_failure:
            instance.report_engine_event(
                "Not retrying run since it failed due to an asset or op failure and run retries "
                "are configured with retry_on_asset_or_op_failure set to false.",
                run,
            )
        else:
            # RETRY-DEBUG: make the otherwise silent "will not retry" decision visible
            _report_retry_debug(
                instance,
                run,
                f"should_retry=False: {WILL_RETRY_TAG}="
                f"{will_retry_tag_value if will_retry_tag_value is not None else '(missing, computed by daemon: false)'}"
                f" (failure_reason={run_failure_reason.value if run_failure_reason else None}).",
                logger,
            )
        return False


def filter_runs_to_should_retry(
    runs: Sequence[DagsterRun],
    instance: DagsterInstance,
    logger: logging.Logger | None = None,
) -> Iterator[DagsterRun]:
    """Return only runs that should retry along with their retry number (1st retry, 2nd, etc.)."""
    for run in runs:
        if should_retry(run, instance, logger):
            yield run


def get_automatically_retried_run_if_exists(
    instance: DagsterInstance, run: DagsterRun, run_group: Sequence[DagsterRun]
) -> DagsterRun | None:
    if run.tags.get(AUTO_RETRY_RUN_ID_TAG) is not None:
        return instance.get_run_by_id(run.tags[AUTO_RETRY_RUN_ID_TAG])
    child_run = next(
        (retried_run for retried_run in run_group if run.run_id == retried_run.parent_run_id), None
    )
    if child_run is not None and child_run.tags.get(RETRY_NUMBER_TAG) is not None:
        # We use the presense of RETRY_NUMBER_TAG to confirm that the child run was launched
        # by the automatic retry daemon. If the child run was launched by the user, the tag
        # should not be present.
        return child_run


def run_was_successfully_retried(run: DagsterRun, instance: DagsterInstance) -> bool:
    """Returns True if the run was successfully retried.
    This includes the following cases:
    - The run initially succeeds
    - The run succeeds on automatic retry
    - The run succeeds on a manual retry.
    """
    run_group = instance.get_run_group(run.run_id)
    if run_group is None:
        return run.status == DagsterRunStatus.SUCCESS
    else:
        runs_in_group = run_group[1]
        return any(r.status == DagsterRunStatus.SUCCESS for r in runs_in_group)


def get_reexecution_strategy(
    run: DagsterRun, instance: DagsterInstance
) -> ReexecutionStrategy | None:
    raw_strategy_tag = run.tags.get(RETRY_STRATEGY_TAG)
    if raw_strategy_tag is None:
        return None

    if raw_strategy_tag not in ReexecutionStrategy.__members__:
        instance.report_engine_event(
            f"Error parsing retry strategy from tag '{RETRY_STRATEGY_TAG}: {raw_strategy_tag}'", run
        )
        return None
    else:
        return ReexecutionStrategy[raw_strategy_tag]


def retry_run(
    failed_run: DagsterRun,
    workspace_context: IWorkspaceProcessContext,
    logger: logging.Logger | None = None,
) -> None:
    """Submit a retry as a re-execute from failure."""
    instance = workspace_context.instance
    workspace = workspace_context.create_request_context()
    if not failed_run.remote_job_origin:
        instance.report_engine_event(
            "Run does not have an external job origin, unable to retry the run.",
            failed_run,
        )
        return

    origin = failed_run.remote_job_origin.repository_origin
    code_location = workspace.get_code_location(origin.code_location_origin.location_name)
    repo_name = origin.repository_name

    if not code_location.has_repository(repo_name):
        instance.report_engine_event(
            f"Could not find repository {repo_name} in location {code_location.name}, unable to"
            " retry the run. It was likely renamed or deleted.",
            failed_run,
        )
        return

    repo = code_location.get_repository(repo_name)

    if not repo.has_job(failed_run.job_name):
        instance.report_engine_event(
            f"Could not find job {failed_run.job_name} in repository {repo_name}, unable"
            " to retry the run. It was likely renamed or deleted.",
            failed_run,
        )
        return

    remote_job = code_location.get_job(
        JobSubsetSelector(
            location_name=origin.code_location_origin.location_name,
            repository_name=repo_name,
            job_name=failed_run.job_name,
            op_selection=failed_run.op_selection,
            asset_selection=(
                None if failed_run.asset_selection is None else list(failed_run.asset_selection)
            ),
            asset_check_selection=(
                None
                if failed_run.asset_check_selection is None
                else list(failed_run.asset_check_selection)
            ),
        )
    )

    try:
        _, run_group = check.not_none(instance.get_run_group(failed_run.run_id))
    except DagsterRunNotFoundError:
        instance.report_engine_event(
            f"Could not find run group for {failed_run.run_id}. This is most likely because the"
            " root run was deleted",
            failed_run,
        )
        return

    run_group_list = list(run_group)

    # it is possible for the daemon to die between creating the run and submitting it. We account for this
    # possibility by checking if the a run already exists in the run group with the parent run id of the
    # failed run and resubmit it if necessary.
    existing_retried_run = get_automatically_retried_run_if_exists(
        instance=instance, run=failed_run, run_group=run_group_list
    )
    if existing_retried_run is not None:
        # RETRY-DEBUG: no new retry run is launched in this branch
        _report_retry_debug(
            instance,
            failed_run,
            f"retry_run: found existing retry run {existing_retried_run.run_id} with status"
            f" {existing_retried_run.status.value}; no new retry run is created"
            + (
                " (it will be submitted now)."
                if existing_retried_run.status == DagsterRunStatus.NOT_STARTED
                else "."
            ),
            logger,
            {"existing retry run": MetadataValue.dagster_run(existing_retried_run.run_id)},
        )
        # ensure the failed_run has the AUTO_RETRY_RUN_ID_TAG set
        if failed_run.tags.get(AUTO_RETRY_RUN_ID_TAG) is None:
            instance.add_run_tags(
                failed_run.run_id, {AUTO_RETRY_RUN_ID_TAG: existing_retried_run.run_id}
            )
        if existing_retried_run.status == DagsterRunStatus.NOT_STARTED:
            # A run already exists but was not submitted.
            instance.submit_run(existing_retried_run.run_id, workspace)
        return

    # At this point we know we need to launch a new run for the retry
    strategy = get_reexecution_strategy(failed_run, instance) or DEFAULT_REEXECUTION_POLICY
    tags = {RETRY_NUMBER_TAG: str(len(run_group_list))}
    new_run = instance.create_reexecuted_run(
        parent_run=failed_run,
        request_context=workspace,
        code_location=code_location,
        remote_job=remote_job,
        strategy=strategy,
        extra_tags=tags,
        use_parent_run_tags=True,
    )
    instance.add_run_tags(failed_run.run_id, {AUTO_RETRY_RUN_ID_TAG: new_run.run_id})

    instance.report_engine_event(
        "Retrying the run",
        failed_run,
        engine_event_data=EngineEventData({"new run": MetadataValue.dagster_run(new_run.run_id)}),
    )
    instance.report_engine_event(
        "Launched as an automatic retry",
        new_run,
        engine_event_data=EngineEventData(
            {"failed run": MetadataValue.dagster_run(failed_run.run_id)}
        ),
    )

    instance.submit_run(new_run.run_id, workspace)


def consume_new_runs_for_automatic_reexecution(
    workspace_process_context: IWorkspaceProcessContext,
    run_records: Sequence[RunRecord],
    logger: logging.Logger,
) -> Iterator[None]:
    """Check which runs should be retried, and retry them.

    It's safe to call this method on the same run multiple times because once a retry run is created,
    it won't create another. The only exception is if the new run gets deleted, in which case we'd
    retry the run again.
    """
    for run in filter_runs_to_should_retry(
        [run_record.dagster_run for run_record in run_records],
        workspace_process_context.instance,
        logger,
    ):
        yield
        try:
            retry_run(run, workspace_process_context, logger)
        except Exception:
            error_info = DaemonErrorCapture.process_exception(
                exc_info=sys.exc_info(),
                logger=logger,
                log_message=f"Failed to retry run {run.run_id}",
            )
            workspace_process_context.instance.report_engine_event(
                "Failed to retry run",
                run,
                engine_event_data=EngineEventData(error=error_info),
            )
            # Since something failed when retrying this run, mark that we will not retry it so that we
            # don't retry it again in the future, and so that the tags reflect the state of the system.
            # We may want to split out the kinds of exceptions and handle them differently in the future so
            # that this can be more resiliant to transient errors. However, this would also require some changes
            # to the EventLogConsumerDaemon so that the cursors are not updated in a way that prevents this run
            # from being processed in the next tick.
            workspace_process_context.instance.add_run_tags(
                run_id=run.run_id, new_tags={WILL_RETRY_TAG: "false"}
            )
