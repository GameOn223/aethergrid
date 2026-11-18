from __future__ import annotations

import threading
from datetime import timedelta

import pytest

from app.application.services.job_execution_service import (
    JobExecutionResult,
    JobExecutionService,
)
from app.application.services.record_job_events_service import (
    RecordJobEventsService,
)
from app.application.services.release_lease_service import (
    ReleaseLeaseService,
)
from app.application.services.renew_lease_service import (
    RenewLeaseService,
)
from app.application.services.report_job_outcome_service import (
    ReportJobOutcomeService,
)
from app.application.workers.worker_execution_loop import (
    WorkerExecutionLoop,
)
from app.domain.entities.job import Job
from app.domain.entities.lease import Lease
from app.domain.entities.node import Node
from app.domain.entities.tenant import DEFAULT_TENANT_ID
from app.domain.entities.worker import Worker
from app.domain.exceptions.no_active_lease_error import (
    NoActiveLeaseError,
)
from app.domain.value_objects.job_id import JobId
from app.domain.value_objects.node_id import NodeId
from app.domain.value_objects.resource_requirements import (
    ResourceRequirements,
)
from app.domain.value_objects.tenant_id import TenantId
from app.domain.value_objects.worker_id import WorkerId
from app.infrastructure.repositories.in_memory_event_repository import (
    InMemoryEventRepository,
)
from app.infrastructure.repositories.in_memory_job_repository import (
    InMemoryJobRepository,
)
from app.infrastructure.repositories.in_memory_lease_repository import (
    InMemoryLeaseRepository,
)
from app.infrastructure.repositories.in_memory_node_repository import (
    InMemoryNodeRepository,
)
from app.infrastructure.repositories.in_memory_worker_repository import (
    InMemoryWorkerRepository,
)
from tests.support.jobs import make_job
from tests.support.nodes import make_node
from tests.support.workers import make_worker

# The scenario: the job's lease expires while the job runs, and
# reconciliation deletes it (RecoverExpiredLeaseService). The job then
# ends before the loop's next renewal, so the renewal thread never sees
# the loss, and the release after the run is the first thing to find it.
# Nothing about the outcome is saved in that case, so the event log must
# not record one either.

OUTCOMES = [
    pytest.param(0, "JobCompleted", id="completed"),
    pytest.param(1, "JobFailed", id="failed"),
]


class _LeaseReclaimedWhileRunning(JobExecutionService):
    """
    Deletes the job's lease the way reconciliation does, then returns
    a result with the given exit code. No subprocess is started.
    """

    def __init__(
        self,
        lease_repository: InMemoryLeaseRepository,
        job_id: JobId,
        tenant_id: TenantId,
        exit_code: int,
    ) -> None:
        super().__init__()
        self._lease_repository = lease_repository
        self._job_id = job_id
        self._tenant_id = tenant_id
        self._exit_code = exit_code

    def execute(
        self,
        command: list[str] | None,
        timeout: timedelta,
        cancel_event: threading.Event | None = None,
        lease_lost_event: threading.Event | None = None,
    ) -> JobExecutionResult:
        self._lease_repository.delete(self._job_id, self._tenant_id)

        return JobExecutionResult(
            exit_code=self._exit_code,
            timed_out=False,
            cancelled=False,
            duration=timedelta(seconds=0),
            stdout="",
            stderr="",
        )


def _make_worker_and_job() -> tuple[Worker, Job, Node]:
    """
    Same shape as _make_worker_and_job in test_worker_execution_loop.py.
    """
    node = make_node(
        id=NodeId.new(),
        capacity=ResourceRequirements(
            cpu_cores=8,
            memory_mib=16384,
            vram_mib=0,
        ),
    )

    worker = make_worker(
        id=WorkerId.new(),
        node=node,
    )
    worker.ready()

    job = make_job(
        id=JobId.new(),
        resources=ResourceRequirements(
            cpu_cores=1,
            memory_mib=512,
            vram_mib=0,
        ),
        command=["true"],
    )

    job.queue()
    job.assign_to(node.id)
    node.allocate(job.resources)

    worker.accept(job)

    return worker, job, node


@pytest.mark.parametrize(("exit_code", "outcome_event"), OUTCOMES)
def test_in_process_loop_records_no_outcome_when_the_lease_is_gone_at_release(
    exit_code: int,
    outcome_event: str,
) -> None:
    worker, job, node = _make_worker_and_job()

    lease_repository = InMemoryLeaseRepository()
    lease_repository.save(
        Lease.create(
            worker_id=worker.id,
            job_id=job.id,
            tenant_id=worker.tenant_id,
        )
    )
    worker_repository = InMemoryWorkerRepository([worker])
    events = InMemoryEventRepository()

    loop = WorkerExecutionLoop(
        worker_repository=worker_repository,
        job_repository=InMemoryJobRepository([job]),
        node_repository=InMemoryNodeRepository([node]),
        lease_repository=lease_repository,
        renew_lease_service=RenewLeaseService(
            lease_repository=lease_repository,
        ),
        release_lease_service=ReleaseLeaseService(
            lease_repository=lease_repository,
            worker_repository=worker_repository,
        ),
        job_execution_service=_LeaseReclaimedWhileRunning(
            lease_repository,
            job.id,
            worker.tenant_id,
            exit_code,
        ),
        record_job_events_service=RecordJobEventsService(
            event_repository=events,
        ),
    )

    with pytest.raises(NoActiveLeaseError):
        loop.execute(worker.id)

    recorded = [event.event_type for event in events.list(DEFAULT_TENANT_ID)]

    assert outcome_event not in recorded


@pytest.mark.parametrize(("exit_code", "outcome_event"), OUTCOMES)
def test_agent_report_records_no_outcome_when_the_lease_is_gone_at_release(
    exit_code: int,
    outcome_event: str,
) -> None:
    """
    The same scenario on the agent path. ReportJobOutcomeService
    releases first and records last, so this already holds.
    """
    worker, job, node = _make_worker_and_job()
    worker.start()

    lease = Lease.create(
        worker_id=worker.id,
        job_id=job.id,
        tenant_id=worker.tenant_id,
    )
    lease_repository = InMemoryLeaseRepository()
    lease_repository.save(lease)
    worker_repository = InMemoryWorkerRepository([worker])
    events = InMemoryEventRepository()

    service = ReportJobOutcomeService(
        worker_repository=worker_repository,
        job_repository=InMemoryJobRepository([job]),
        node_repository=InMemoryNodeRepository([node]),
        lease_repository=lease_repository,
        release_lease_service=ReleaseLeaseService(
            lease_repository=lease_repository,
            worker_repository=worker_repository,
        ),
        record_job_events_service=RecordJobEventsService(
            event_repository=events,
        ),
    )

    lease_repository.delete(job.id, worker.tenant_id)

    report = service.complete if exit_code == 0 else service.fail

    with pytest.raises(NoActiveLeaseError):
        report(
            worker.id,
            job.id,
            worker.tenant_id,
            lease.id,
            exit_code=exit_code,
        )

    recorded = [event.event_type for event in events.list(DEFAULT_TENANT_ID)]

    assert outcome_event not in recorded
