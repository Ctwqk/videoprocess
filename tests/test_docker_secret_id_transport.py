"""Offline Engine transport and atomic AutoFlow spec regression tests."""

import copy
import json
from pathlib import Path
import runpy
import subprocess
import sys
import time
from unittest.mock import patch

import pytest


HELPER = runpy.run_path(str(Path(__file__).resolve().parents[1] / "deploy/swarm/worker-admission-transaction.py"))
ERROR = HELPER["TransactionError"]
SERVICE = "s" * 25
SECRET_ID = "b" * 25
GENERATION = "c-0123456789abcdef0123"
NAME = "vp-wc-orchestrator-" + GENERATION
IMAGE = "vp-backend-api:deploy-0123456789ab"


def record():
    return {
        "ID": SERVICE,
        "Version": {"Index": 71},
        "Spec": {
            "Name": "vp-autoflow-api-swarm",
            "Labels": {"preserved": "yes"},
            "Mode": {"Replicated": {"Replicas": 1}},
            "EndpointSpec": {"Mode": "vip", "Ports": [{"TargetPort": 8080}]},
            "UpdateConfig": {"Order": "stop-first", "Parallelism": 1, "Delay": 17},
            "RollbackConfig": {"Order": "stop-first"},
            "TaskTemplate": {
                "ForceUpdate": 3,
                "Resources": {"Limits": {"MemoryBytes": 123456}},
                "RestartPolicy": {"Condition": "any"},
                "Networks": [{"Target": "n" * 25, "Aliases": ["kept"]}],
                "Placement": {"Constraints": ["old==true"], "Preferences": [{"Spread": {"SpreadDescriptor": "node.id"}}]},
                "ContainerSpec": {
                    "Image": "vp-backend-api:deploy-111111111111",
                    "User": "",
                    "Env": ["KEEP=private-value", "WORKER_ORCHESTRATOR_CONTROL_GENERATION=old"],
                    "Mounts": [{"Type": "bind", "Source": "/kept", "Target": "/kept"}],
                    "Configs": [{"ConfigID": "c" * 25}],
                    "Healthcheck": {"StartInterval": 123},
                    "Secrets": [
                        {"SecretID": "x" * 25, "SecretName": "unrelated", "File": {"Name": "other"}},
                        {"SecretID": "o" * 25, "SecretName": "vp-wc-orchestrator-old", "File": {"Name": "worker-orchestrator-database-url"}},
                    ],
                },
            },
        },
    }


def target(source, order="start-first"):
    return HELPER["_autoflow_update_spec"](
        source, SERVICE, IMAGE, order, NAME + "|" + SECRET_ID + "|" + GENERATION,
        "", "health-command", "colima-127",
    )


@pytest.mark.parametrize("order", ["start-first", "stop-first"])
def test_atomic_spec_pins_ids_and_preserves_all_unrelated_fields(order):
    source = record()
    before = copy.deepcopy(source)
    desired = target(source, order)
    expected = copy.deepcopy(source["Spec"])
    expected["UpdateConfig"]["Order"] = order
    expected["TaskTemplate"]["Placement"]["Constraints"] = [
        "node.labels.vp.runtime==true", "node.hostname==colima-127",
    ]
    container = expected["TaskTemplate"]["ContainerSpec"]
    container["Image"] = IMAGE
    container["Env"] = [
        "KEEP=private-value",
        "WORKER_ORCHESTRATOR_DATABASE_URL_FILE=/run/secrets/worker-orchestrator-database-url",
        "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + GENERATION,
    ]
    container["Secrets"][-1] = {
        "SecretID": SECRET_ID, "SecretName": NAME,
        "File": {"Name": "worker-orchestrator-database-url", "UID": "0", "GID": "0", "Mode": 256},
    }
    container["Healthcheck"].update(
        Test=["CMD-SHELL", "health-command"], Interval=10_000_000_000,
        Timeout=3_000_000_000, Retries=6, StartPeriod=10_000_000_000,
    )
    assert desired == expected
    assert source == before


@pytest.mark.parametrize("fault", ["service", "duplicate_env", "unknown_secret", "duplicate_secret", "user"])
def test_atomic_spec_rejects_ambiguous_or_rebound_state(fault):
    source = record()
    container = source["Spec"]["TaskTemplate"]["ContainerSpec"]
    if fault == "service":
        source["ID"] = "q" * 25
    elif fault == "duplicate_env":
        container["Env"].append("KEEP=other")
    elif fault == "unknown_secret":
        container["Secrets"][-1]["SecretName"] = "operator-credential"
    elif fault == "duplicate_secret":
        container["Secrets"].append(copy.deepcopy(container["Secrets"][-1]))
    else:
        container["User"] = "unqualified-user"
    with pytest.raises(ERROR):
        target(source)


def response(status, value):
    data = json.dumps(value).encode()
    return f"HTTP/1.1 {status} Result\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode() + data


@pytest.mark.parametrize("create", [True, False])
def test_engine_wire_pins_ids_without_name_resolution(create):
    calls = []
    spec = target(record())
    path = "/services/create" if create else f"/services/{SERVICE}/update?version=71&registryAuthFrom=spec"
    status = 201 if create else 200

    def exchange(request):
        calls.append(request)
        return response(status, {"ID": SERVICE} if create else {})

    with patch.dict(HELPER["_engine_service_post"].__globals__, _engine_exchange=exchange):
        result = HELPER["_engine_service_post"](path, spec, status)
    assert result == ({"ID": SERVICE} if create else {})
    assert len(calls) == 1
    wire = calls[0]
    headers, body = wire.split(b"\r\n\r\n", 1)
    assert headers.startswith(f"POST /v1.52{path} HTTP/1.1\r\n".encode())
    assert b"Connection: close" in headers
    assert json.loads(body) == spec
    assert json.loads(body)["TaskTemplate"]["ContainerSpec"]["Secrets"][-1]["SecretID"] == SECRET_ID


@pytest.mark.parametrize("fault", ["lost", "conflict", "truncated", "invalid_json", "oversize"])
def test_engine_transport_never_retries_or_exposes_raw_failures(fault):
    calls = []

    def exchange(request):
        calls.append(request)
        if fault == "lost":
            raise subprocess.TimeoutExpired("docker", 30, output=b"private-value", stderr=b"private-value")
        raw = response(409, {"message": "private-value"})
        if fault == "truncated":
            raw = b"HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\n{}"
        elif fault == "invalid_json":
            raw = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx"
        elif fault == "oversize":
            raw = b"x" * (HELPER["MAX_DOCUMENT_BYTES"] + 1)
        return raw

    with patch.dict(HELPER["_engine_service_post"].__globals__, _engine_exchange=exchange), pytest.raises(ERROR) as raised:
        HELPER["_engine_service_post"](f"/services/{SERVICE}/update?version=71&registryAuthFrom=spec", record()["Spec"], 200)
    assert len(calls) == 1
    assert "private-value" not in str(raised.value)


def test_engine_output_cap_stops_and_reaps_writer_before_timeout():
    original = subprocess.Popen
    children = []

    def popen(args, **kwargs):
        assert args == ["docker", "system", "dial-stdio"]
        assert kwargs["stderr"] == subprocess.DEVNULL
        child = original([sys.executable, "-c", "import sys,time; sys.stdin.buffer.read(); sys.stdout.buffer.write(b'x'*(2*1024*1024)); sys.stdout.buffer.flush(); time.sleep(3)"], **kwargs)
        children.append(child)
        return child

    started = time.monotonic()
    with patch.object(subprocess, "Popen", popen), pytest.raises(ERROR):
        HELPER["_engine_service_post"]("/services/create", {}, 201)
    assert time.monotonic() - started < 1
    assert len(children) == 1 and children[0].poll() is not None
