from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import tomllib
import venv
from collections import Counter
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile

# Public entry points, including the module paths used before package re-exports.
PUBLIC_IMPORTS = {
    "aws": {
        "aws.dynamodb.get": ("AsyncDynamoDbGetBatcher", "GetItem"),
        "aws.dynamodb.write": ("AsyncDynamoDbWriteBatcher", "WriteOperation"),
        "aws.dynamodb.exceptions": ("DynamoDBRetryError",),
        "aws.kinesis": ("AsyncKinesisBatcher", "KinesisRecord", "RecordBatchError"),
        "aws.kinesis.write": ("AsyncKinesisBatcher", "KinesisRecord"),
        "aws.firehose": ("AsyncFirehoseBatcher", "RecordBatchError"),
        "aws.firehose.write": ("AsyncFirehoseBatcher",),
    },
    "sqlalchemy": {"sqlalchemy.write": ("AsyncSqlalchemyWriteBatcher",)},
    "scylla": {"scylladb.update": ("AsyncScyllaDbWriteBatcher", "WriteOperation")},
    "keras": {"ml.keras.model": ("KerasAsyncBatcher",)},
    "sklearn": {"ml.sklearn.model": ("SklearnAsyncBatcher",)},
    "mongodb": {
        "mongodb": ("AsyncMongoDbWriteBatcher", "MongoDbNotExecutedError"),
        "mongodb.write": ("AsyncMongoDbWriteBatcher", "MongoDbNotExecutedError"),
    },
    "kafka": {
        "kafka": ("AsyncKafkaBatcher", "KafkaRecord"),
        "kafka.producer": ("AsyncKafkaBatcher", "KafkaRecord"),
    },
    "pinecone": dict.fromkeys(
        ("pinecone", "pinecone.vectors"),
        ("AsyncPineconeUpsertBatcher", "AsyncPineconeFetchBatcher", "AsyncPineconeDeleteBatcher"),
    ),
}


def compact(value: str) -> str:
    return "".join(value.split()).replace('"', "'")


def validate_metadata(metadata, project: dict) -> None:
    for header, field in (("Name", "name"), ("Version", "version"), ("Requires-Python", "requires-python")):
        if compact(metadata.get(header, "")) != compact(project[field]):
            raise ValueError(f"Incorrect distribution metadata: {header}")
    extras = project["optional-dependencies"]
    if Counter(metadata.get_all("Provides-Extra", [])) != Counter(extras.keys()):
        raise ValueError("Distribution extras differ from pyproject.toml")
    expected = list(project.get("dependencies", []))
    for extra, requirements in extras.items():
        for requirement in requirements:
            dependency, separator, marker = requirement.partition(";")
            condition = f"({marker.strip()}) and " if separator else ""
            expected.append(f"{dependency}; {condition}extra == '{extra}'")
    if Counter(map(compact, metadata.get_all("Requires-Dist", []))) != Counter(map(compact, expected)):
        raise ValueError("Distribution requirements differ from pyproject.toml (including extra markers)")


def validate_wheel(wheel: Path, root: Path, project: dict) -> None:
    expected = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in (root / "async_batcher").rglob("*.py")
    }
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        metadata_files = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_files) != 1:
            raise ValueError("Wheel must contain exactly one distribution metadata file")
        validate_metadata(BytesParser().parsebytes(archive.read(metadata_files[0])), project)
        packaged = {name for name in names if name.startswith("async_batcher/") and name.endswith(".py")}
        if packaged != expected.keys():
            raise ValueError(f"Wheel package files differ from source: {sorted(packaged ^ expected.keys())}")
        for name, contents in expected.items():
            if archive.read(name) != contents:
                raise ValueError(f"Wheel contains different source bytes: {name}")
        license_path = metadata_files[0].removesuffix("METADATA") + "licenses/LICENSE"
        if archive.read(license_path) != (root / "LICENSE").read_bytes():
            raise ValueError("Wheel license differs from source")


def verify_origin(module, site_packages: Path) -> None:
    if not Path(module.__file__).resolve().is_relative_to(site_packages.resolve()):
        raise ValueError(
            f"Imported {module.__name__} from outside installed site-packages: {module.__file__}"
        )


async def batching_probe() -> None:
    from async_batcher import AsyncBatcher, AsyncBatchException, QueueFullException

    assert issubclass(QueueFullException, AsyncBatchException)
    batches = []

    class DoubleBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            batches.append(batch)
            return [item * 2 for item in batch]

    batcher = DoubleBatcher(max_batch_size=3, max_queue_time=1)
    try:
        async with asyncio.timeout(10):
            results = await asyncio.gather(*(batcher.process(item) for item in range(6)))
        assert results == [0, 2, 4, 6, 8, 10]
        assert batches == [[0, 1, 2], [3, 4, 5]]
    finally:
        await batcher.stop(force=True, timeout=5)


def probe(expectations: Path, adapter: str | None) -> None:
    expected = json.loads(expectations.read_text())
    root = Path(expected["checkout"])
    if not sys.flags.isolated or Path.cwd().is_relative_to(root) or str(root) in sys.path:
        raise ValueError(
            "Installed-package probes require Python -I and a working directory outside checkout"
        )
    distribution = importlib.metadata.distribution(expected["project"]["name"])
    validate_metadata(distribution.metadata, expected["project"])
    if adapter is None:
        installed = {dist.metadata["Name"] for dist in importlib.metadata.distributions()}
        if installed != {expected["project"]["name"]}:
            raise ValueError(f"The core environment contains unexpected dependencies: {sorted(installed)}")
    for module_name, symbols in PUBLIC_IMPORTS.get(adapter, {}).items():
        module = importlib.import_module("async_batcher." + module_name)
        for symbol in symbols:
            if not hasattr(module, symbol):
                raise ValueError(f"Missing public import: {module.__name__}.{symbol}")
    asyncio.run(batching_probe())
    site_packages = Path(sysconfig.get_path("purelib"))
    for name, module in list(sys.modules.items()):
        if name == "async_batcher" or name.startswith("async_batcher."):
            verify_origin(module, site_packages)
    print(f"Installed {adapter or 'dependency-free core'} probe passed from {site_packages}")


def run(command: list[str], cwd: Path, env: dict) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True, timeout=180)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate installed wheels outside the source checkout")
    parser.add_argument("--adapter", choices=PUBLIC_IMPORTS)
    parser.add_argument("--probe", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--uv", default="uv")
    args = parser.parse_args()
    if args.probe:
        probe(args.probe, args.adapter)
        return
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    if project.get("dependencies") or set(project["optional-dependencies"]) != PUBLIC_IMPORTS.keys():
        raise ValueError("Update the distribution probes for changed core dependencies or adapter extras")
    uv = shutil.which(args.uv)
    if uv is None:
        raise ValueError("uv must be installed before running distribution checks")
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"}
    }
    with tempfile.TemporaryDirectory(prefix="batcher-distribution-") as temporary:
        work = Path(temporary).resolve()
        if work.is_relative_to(root):
            raise ValueError("Temporary directory must be outside the checkout")
        expected = work / "expected.json"
        expected.write_text(json.dumps({"checkout": str(root), "project": project}))
        direct = work / "direct"
        run([uv, "build", "--no-sources", "--wheel", "--out-dir", str(direct), str(root)], work, env)
        wheels = [next(direct.glob("*.whl"))]
        if args.adapter is None:
            run(
                [uv, "build", "--no-sources", "--sdist", "--out-dir", str(work / "sdist"), str(root)],
                work,
                env,
            )
            sdist = next((work / "sdist").glob("*.tar.gz"))
            run(
                [uv, "build", "--no-sources", "--wheel", "--out-dir", str(work / "rebuilt"), str(sdist)],
                work,
                env,
            )
            wheels.append(next((work / "rebuilt").glob("*.whl")))
        for index, wheel in enumerate(wheels):
            validate_wheel(wheel, root, project)
            if args.adapter:
                # Reuse the selected SDKs after tests; replace only our editable package.
                python = Path(sys.executable)
            else:
                environment = work / f"core-{index}"
                venv.EnvBuilder(with_pip=False).create(environment)
                python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            run(
                [uv, "pip", "install", "--python", str(python), "--no-deps", "--reinstall", str(wheel)],
                work,
                env,
            )
            command = [str(python), "-I", str(Path(__file__).resolve()), "--probe", str(expected)]
            if args.adapter:
                command += ["--adapter", args.adapter]
            run(command, work, env)


if __name__ == "__main__":
    main()
