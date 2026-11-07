from __future__ import annotations

import json
import os
from pathlib import Path


def plan(event: str, changed: set[str]) -> dict:
    full = event in {"schedule", "workflow_dispatch"} or "infrastructure" in changed
    versions = ["3.11", "3.12", "3.13", "3.14", "3.15"] if full or event == "push" else ["3.11", "3.15"]
    core = [{"os": "ubuntu-24.04", "python": version} for version in versions]
    if full or event == "push" or "core" in changed:
        core.extend({"os": runner, "python": "3.15"} for runner in ("macos-15", "macos-15-intel"))
    adapters = []
    services = []
    for suite, groups in (
        ("aws", "test"),
        ("kafka", "test"),
        ("sqlalchemy", "test adapter-test"),
        ("scylla", "test"),
        ("mongodb", "test"),
        ("keras", "test ml-test"),
        ("sklearn", "test"),
        ("pinecone", "test"),
    ):
        if not full and suite not in changed:
            continue
        for version in ["3.11", "3.15"] if full else ["3.12"]:
            adapters.append({"suite": suite, "python": version, "groups": groups})
        if suite in {"aws", "sqlalchemy", "scylla", "mongodb", "kafka"}:
            services.append(
                {
                    "suite": "scylladb" if suite == "scylla" else suite,
                    "extra": suite,
                    "groups": groups,
                    "compose": {
                        "aws": "dynamodb",
                        "sqlalchemy": "postgres",
                        "scylla": "scylladb",
                        "mongodb": "mongodb",
                        "kafka": "kafka",
                    }[suite],
                }
            )
    return {
        "core": {"include": core},
        "adapters": {"include": adapters},
        "services": {"include": services},
        "has_adapters": bool(adapters),
        "has_services": bool(services),
    }


if __name__ == "__main__":
    result = plan(os.environ["GITHUB_EVENT_NAME"], set(json.loads(os.environ.get("CHANGED_FILTERS") or "[]")))
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        for name, value in result.items():
            output.write(f"{name}={json.dumps(value, separators=(',', ':'))}\n")
    print(json.dumps(result, indent=2))
