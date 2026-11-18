from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from scripts.run_agent import run_job


def test_the_failure_report_says_why_the_command_could_not_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """
    ADR 0081, follow-up. The agent knows why the command could not
    start, but the report it posts carries only the exit code and the
    lease, so the server never learns the reason. The report should
    carry it, naming the program and never the arguments, which can
    hold secrets.
    """
    monkeypatch.setattr("scripts.run_agent.RENEWAL_INTERVAL_SECONDS", 3600)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={})

    client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    )
    program = str(tmp_path / "no-such-binary")
    job = {
        "id": "job-1",
        "command": [program, "--token", "not-a-real-secret"],
        "lease_id": "lease-abc",
        "execution_timeout_seconds": 30,
    }

    run_job(client, "worker-1", job)

    assert requests[-1].url.path == "/workers/worker-1/jobs/job-1/fail"

    body = json.loads(requests[-1].content)
    reason = body.get("failure_reason")

    assert isinstance(reason, str)
    assert program in reason
    assert "not-a-real-secret" not in reason
