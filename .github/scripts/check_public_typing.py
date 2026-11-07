"""Check public types from an installed wheel with positive and negative consumers."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import venv
from collections import Counter
from pathlib import Path


def expected_errors(source: str) -> Counter[tuple[int, str]]:
    expected: Counter[tuple[int, str]] = Counter()
    for number, line in enumerate(source.splitlines(), 1):
        if "# error:" not in line:
            continue
        marker = line.split("# error:", 1)[1].strip()
        codes = [code.strip() for code in marker.split(",")]
        if not codes or any(not re.fullmatch(r"[a-z][a-z0-9-]*", code) for code in codes):
            raise ValueError(f"Invalid expected-error marker on line {number}")
        expected.update((number, code) for code in codes)
    return expected


def diagnostics(result: subprocess.CompletedProcess[str], fixture: Path) -> Counter[tuple[int, str]]:
    if result.returncode not in (0, 1) or result.stderr:
        raise ValueError(f"Type checker failed to run: {result.returncode}\n{result.stdout}\n{result.stderr}")
    actual: Counter[tuple[int, str]] = Counter()
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid checker JSON: {line}") from error
        if not isinstance(entry, dict) or entry.get("severity") not in {"error", "note"}:
            raise ValueError(f"Unexpected checker diagnostic: {entry}")
        if entry["severity"] == "note":
            continue
        filename = entry.get("file")
        if not isinstance(filename, str) or not filename:
            raise ValueError(f"Missing diagnostic filename: {entry}")
        location = Path(filename)
        if not location.is_absolute():
            location = fixture.parent / location
        if (
            location.resolve() != fixture.resolve()
            or type(entry.get("line")) is not int
            or entry["line"] < 1
            or not isinstance(entry.get("code"), str)
        ):
            raise ValueError(f"Unrecognized error location/code: {entry}")
        actual[(entry["line"], entry["code"])] += 1
    if bool(actual) != (result.returncode == 1):
        raise ValueError("Checker exit status disagrees with its diagnostics")
    return actual


def verify_fixture(result: subprocess.CompletedProcess[str], fixture: Path, *, negative: bool) -> None:
    expected = expected_errors(fixture.read_text())
    if bool(expected) != negative:
        raise ValueError("Negative fixtures need expected errors; positive fixtures must have none")
    actual = diagnostics(result, fixture)
    if actual != expected:
        raise ValueError(
            f"Public typing regression in {fixture.name}: missing={expected - actual}, "
            f"unexpected={actual - expected}\n{result.stdout}"
        )


def run(command: list[str], work: Path, env: dict[str, str], *, quiet: bool = False) -> None:
    subprocess.run(
        command,
        cwd=work,
        env=env,
        check=True,
        timeout=180,
        stdout=subprocess.DEVNULL if quiet else None,
    )


def dependency_export(uv: str, root: Path, destination: Path, adapter: str) -> list[str]:
    if adapter not in {"aws", "kafka"}:
        raise ValueError(f"Unsupported typing adapter: {adapter}")
    command = [
        uv,
        "export",
        "--project",
        str(root),
        "--locked",
        "--no-default-groups",
        "--no-emit-project",
        "--extra",
        adapter,
        "--output-file",
        str(destination),
    ]
    if adapter == "aws":
        command.extend(["--group", "typing"])
    return command


def checker_config(adapter: str | None) -> str:
    config = "[mypy]\nstrict = True\npython_version = 3.11\n"
    if adapter == "kafka":
        # aiokafka has inline types but no PEP 561 marker. Analyze the installed
        # SDK's annotations instead of treating its producer/results as Any.
        config += "\n[mypy-aiokafka.*]\nfollow_untyped_imports = True\n"
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--adapter", choices=("aws", "kafka"))
    args = parser.parse_args()
    uv = shutil.which(args.uv)
    if uv is None:
        raise ValueError("uv must be installed before checking public types")
    root = Path(__file__).resolve().parents[2]
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "MYPYPATH", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT"}
    }
    with tempfile.TemporaryDirectory(prefix="batcher-types-") as temporary:
        work = Path(temporary).resolve()
        if work.is_relative_to(root):
            raise ValueError("Typing consumers must run outside the checkout")
        fixtures = []
        for name in ("valid.py", "invalid.py"):
            fixture = work / name
            source_name = f"{args.adapter}_{name}" if args.adapter else name
            shutil.copyfile(root / "tests" / "typing" / source_name, fixture)
            fixtures.append(fixture)
        run([uv, "build", "--no-sources", "--wheel", "--out-dir", str(work / "dist"), str(root)], work, env)
        wheel = next((work / "dist").glob("*.whl"))
        environment = work / "installed"
        venv.EnvBuilder(with_pip=False).create(environment)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        if args.adapter:
            requirements = work / "sdk-requirements.txt"
            run(dependency_export(uv, root, requirements, args.adapter), work, env, quiet=True)
            run(
                [uv, "pip", "install", "--python", str(python), "--require-hashes", "-r", str(requirements)],
                work,
                env,
            )
        run([uv, "pip", "install", "--python", str(python), "--no-deps", str(wheel)], work, env)
        imports = "import pathlib,async_batcher; "
        if args.adapter:
            module = "async_batcher.aws.kinesis" if args.adapter == "aws" else "async_batcher.kafka"
            imports += f"import importlib; importlib.import_module({module!r}); "
        query = subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                imports + "print(pathlib.Path(async_batcher.__file__).parent)",
            ],
            cwd=work,
            env=env,
            text=True,
            capture_output=True,
            check=True,
            timeout=30,
        )
        package = Path(query.stdout.strip()).resolve()
        if not package.is_relative_to(environment):
            raise ValueError(f"Typing probe imported outside its installed environment: {package}")
        marker = package / "py.typed"
        marker_bytes = marker.read_bytes()
        config = work / "mypy.ini"
        config.write_text(checker_config(args.adapter))
        command = [
            sys.executable,
            "-m",
            "mypy",
            "--config-file",
            str(config),
            "--python-executable",
            str(python),
            "--no-incremental",
            "--cache-dir",
            str(work / "cache"),
            "--output=json",
        ]

        def check(fixture: Path) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [*command, str(fixture)],
                cwd=work,
                env=env,
                text=True,
                capture_output=True,
                timeout=120,
            )

        for fixture, negative in zip(fixtures, (False, True), strict=True):
            verify_fixture(check(fixture), fixture, negative=negative)
        # Prove that local/editable sources or cached Any types cannot mask a
        # missing marker in the installed distribution used by this checker.
        marker.unlink()
        try:
            missing_marker = diagnostics(check(fixtures[0]), fixtures[0])
            if not any(code == "import-untyped" for _, code in missing_marker):
                raise ValueError("Removing installed py.typed did not fail the positive consumer")
        finally:
            marker.write_bytes(marker_bytes)
        print(
            f"Public typing ({args.adapter or 'core'}) passed "
            f"with mypy {importlib.metadata.version('mypy')}: "
            "positive consumer, exact negative diagnostics, installed-marker negative control"
        )


if __name__ == "__main__":
    main()
