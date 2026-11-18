from __future__ import annotations

from pathlib import Path

from app.application.services.job_execution_service import (
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
from app.application.workers.worker_execution_loop import (
    WorkerExecutionLoop,
)
from app.domain.entities.lease import Lease
from app.domain.entities.tenant import DEFAULT_TENANT_ID
from app.domain.value_objects.job_id import JobId
from app.domain.value_objects.node_id import NodeId
from app.domain.value_objects.resource_requirements import (
    ResourceRequirements,
)
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


def test_job_failed_event_says_why_the_command_could_not_start(
    tmp_path: Path,
) -> None:
    """
    ADR 0081, follow-up. The run is recorded as failed, but the reason
    only goes to the log, with no job id, and the JobFailed event is
    empty. Event.payload is already stored and served by /events and
    /jobs/{id}/history, so the reason belongs there. It names the
    program and never the arguments.
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

    program = str(tmp_path / "no-such-binary")
    job = make_job(
        id=JobId.new(),
        resources=ResourceRequirements(
            cpu_cores=1,
            memory_mib=512,
            vram_mib=0,
        ),
        command=[program, "--token", "not-a-real-secret"],
    )
    job.queue()
    job.assign_to(node.id)
    node.allocate(job.resources)
    worker.accept(job)

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
        job_execution_service=JobExecutionService(),
        record_job_events_service=RecordJobEventsService(
            event_repository=events,
        ),
    )

    loop.execute(worker.id)

    failed = [event for event in events.list(DEFAULT_TENANT_ID) if event.event_type == "JobFailed"]

    assert len(failed) == 1

    reason = failed[0].payload.get("reason")

    assert reason is not None
    assert program in reason
    assert "not-a-real-secret" not in reason
