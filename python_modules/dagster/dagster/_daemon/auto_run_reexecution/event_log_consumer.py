import logging
import os
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING

import dagster._check as check
from dagster import DagsterEventType
from dagster._core.events import EngineEventData
from dagster._core.instance import DagsterInstance
from dagster._core.storage.dagster_run import RunRecord, RunsFilter
from dagster._core.workspace.context import IWorkspaceProcessContext
from dagster._daemon.auto_run_reexecution.auto_run_reexecution import (
    RETRY_DEBUG_PREFIX,
    consume_new_runs_for_automatic_reexecution,
)
from dagster._daemon.daemon import IntervalDaemon
from dagster._daemon.utils import DaemonErrorCapture

if TYPE_CHECKING:
    from dagster._core.events.log import EventLogEntry

_INTERVAL_SECONDS = int(os.environ.get("DAGSTER_EVENT_LOG_CONSUMER_DAEMON_INTERVAL_SECONDS", "5"))
_EVENT_LOG_FETCH_LIMIT = int(os.environ.get("DAGSTER_EVENT_LOG_CONSUMER_DAEMON_FETCH_LIMIT", "500"))

DAGSTER_EVENT_TYPES = [DagsterEventType.RUN_FAILURE, DagsterEventType.RUN_SUCCESS]


class EventLogConsumerDaemon(IntervalDaemon):
    def __init__(
        self,
        interval_seconds: int = _INTERVAL_SECONDS,
        event_log_fetch_limit: int = _EVENT_LOG_FETCH_LIMIT,
    ):
        super().__init__(interval_seconds=interval_seconds)
        self._event_log_fetch_limit = event_log_fetch_limit

    @classmethod
    def daemon_type(cls) -> str:
        return "EVENT_LOG_CONSUMER"

    @property
    def handle_updated_runs_fns(
        self,
    ) -> Sequence[
        Callable[[IWorkspaceProcessContext, Sequence[RunRecord], logging.Logger], Iterator]
    ]:
        """List of functions that will be called with the list of run records that have new events."""
        return [consume_new_runs_for_automatic_reexecution]

    def run_iteration(self, workspace_process_context: IWorkspaceProcessContext):
        instance = workspace_process_context.instance
        # get the persisted cursor for each event type
        persisted_cursors = _fetch_persisted_cursors(instance, DAGSTER_EVENT_TYPES, self._logger)

        # Fetch overall max only if needed to initialize a missing cursor. After initialization,
        # cursors are advanced based solely on observed events, so this value is not needed.
        overall_max_event_id: int | None = None

        events: list[EventLogEntry] = []
        new_cursors: dict[
            DagsterEventType, int
        ] = {}  # keep these in memory until we handle the events
        for event_type in DAGSTER_EVENT_TYPES:
            yield

            cursor = persisted_cursors[event_type]
            if cursor is None:
                # if we don't have a cursor for this event type, start at the top of the event log and ignore older events. Otherwise enabling the daemon would result in retrying all old runs.
                if overall_max_event_id is None:
                    overall_max_event_id = instance.event_log_storage.get_maximum_record_id() or 0
                cursor = overall_max_event_id

            events_by_log_id_for_type = instance.event_log_storage.get_logs_for_all_runs_by_log_id(
                after_cursor=cursor,
                dagster_event_type={event_type},
                limit=self._event_log_fetch_limit,
            )

            events.extend(events_by_log_id_for_type.values())

            # calculate the new cursor for this event type
            new_cursors[event_type] = get_new_cursor(
                cursor,
                list(events_by_log_id_for_type.keys()),
            )

        if events:
            run_ids = list({event.run_id for event in events})
            run_records = instance.get_run_records(filters=RunsFilter(run_ids=run_ids))

            # RETRY-DEBUG: trace every RUN_FAILURE event the daemon consumes, together with the
            # run status it sees at that moment, so that failed runs without any debug event can
            # still be matched against the daemon log.
            status_by_run_id = {
                record.dagster_run.run_id: record.dagster_run.status.value for record in run_records
            }
            for event in events:
                if event.dagster_event_type == DagsterEventType.RUN_FAILURE:
                    self._logger.info(
                        f"{RETRY_DEBUG_PREFIX} consuming RUN_FAILURE event for run {event.run_id},"
                        f" run status seen: {status_by_run_id.get(event.run_id, 'RUN RECORD NOT FOUND')}"
                    )

            # call each handler with the list of runs that have events
            for fn in self.handle_updated_runs_fns:
                try:
                    yield from fn(workspace_process_context, run_records, self._logger)
                except Exception:
                    error_info = DaemonErrorCapture.process_exception(
                        sys.exc_info(),
                        logger=self._logger,
                        log_message=f"Error calling event event log consumer handler: {fn.__name__}",  # ty: ignore[unresolved-attribute]
                    )
                    # RETRY-DEBUG: the cursor below is persisted anyway, so runs in this batch that
                    # were not handled yet will never be looked at again.
                    for record in run_records:
                        try:
                            instance.report_engine_event(
                                f"{RETRY_DEBUG_PREFIX} event log consumer handler"
                                f" {fn.__name__} raised an exception while processing a batch"  # ty: ignore[unresolved-attribute]
                                " containing this run. The event cursor will still advance, so"
                                " this run may never be retried.",
                                record.dagster_run,
                                engine_event_data=EngineEventData(error=error_info),
                            )
                        except Exception:
                            self._logger.exception(
                                f"{RETRY_DEBUG_PREFIX} failed to report handler exception for"
                                f" run {record.dagster_run.run_id}"
                            )

            self._logger.debug(
                f"{RETRY_DEBUG_PREFIX} cursors {_format_cursors(persisted_cursors)} ->"
                f" {_format_cursors(new_cursors)}, {len(events)} events, {len(run_ids)} runs"
            )

        # persist cursors now that we've processed all the events through the handlers
        _persist_cursors(instance, new_cursors)


def _format_cursors(cursors: Mapping[DagsterEventType, int | None]) -> str:
    return ", ".join(f"{event_type.value}={cursor}" for event_type, cursor in cursors.items())


def _create_cursor_key(event_type: DagsterEventType) -> str:
    check.inst_param(event_type, "event_type", DagsterEventType)

    return f"EVENT_LOG_CONSUMER_CURSOR-{event_type.value}"


def _fetch_persisted_cursors(
    instance: DagsterInstance, event_types: Sequence[DagsterEventType], logger: logging.Logger
) -> dict[DagsterEventType, int | None]:
    check.inst_param(instance, "instance", DagsterInstance)
    check.sequence_param(event_types, "event_types", of_type=DagsterEventType)

    # get the persisted cursor for each event type
    persisted_cursors = instance.daemon_cursor_storage.get_cursor_values(
        {_create_cursor_key(event_type) for event_type in event_types}
    )

    fetched_cursors: dict[DagsterEventType, int | None] = {}
    for event_type in event_types:
        raw_cursor_value = persisted_cursors.get(_create_cursor_key(event_type))

        if raw_cursor_value is None:
            logger.warn(f"No cursor for event type {event_type}, ignoring older events")
            fetched_cursors[event_type] = None
        else:
            try:
                cursor_value = int(raw_cursor_value)
            except ValueError:
                logger.exception(f"Invalid cursor for event_type {event_type}: {raw_cursor_value}")
                raise
            fetched_cursors[event_type] = cursor_value

    return fetched_cursors


def _persist_cursors(instance: DagsterInstance, cursors: Mapping[DagsterEventType, int]) -> None:
    check.inst_param(instance, "instance", DagsterInstance)
    check.mapping_param(cursors, "cursors", key_type=DagsterEventType, value_type=int)

    if cursors:
        instance.daemon_cursor_storage.set_cursor_values(
            {
                _create_cursor_key(event_type): str(cursor_value)
                for event_type, cursor_value in cursors.items()
            }
        )


def get_new_cursor(
    persisted_cursor: int,
    new_event_ids: Sequence[int],
) -> int:
    """Return the new cursor value for an event type.

    The cursor advances to the maximum id of any newly fetched events, or stays at the current
    value if no new events were found.
    """
    check.int_param(persisted_cursor, "persisted_cursor")
    check.sequence_param(new_event_ids, "new_event_ids", of_type=int)

    if new_event_ids:
        # these should be ordered, but we won't assume
        max_new_event_id = max(new_event_ids)

        check.invariant(
            max_new_event_id > persisted_cursor,
            f"The new cursor {max_new_event_id} should be greater than the previous {persisted_cursor}",
        )

        return max_new_event_id
    else:
        return persisted_cursor
