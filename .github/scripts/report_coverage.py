from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path

ADAPTER_PATHS = {
    "aws": "aws",
    "kafka": "kafka",
    "sqlalchemy": "sqlalchemy",
    "scylla": "scylladb",
    "mongodb": "mongodb",
    "keras": "ml/keras",
    "sklearn": "ml/sklearn",
    "pinecone": "pinecone",
}


def planned_artifacts(
    core: dict, adapters: dict, services: dict, *, examples: bool = False, provider_recipes: bool = False
) -> tuple[list[str], set[str]]:
    """Use the exact test matrix, including each OS/interpreter and live service."""
    if not core["include"]:
        raise ValueError("The coverage plan must include the core gate")
    names = [f"coverage-data-core-{row['os']}-py{row['python']}" for row in core["include"]]
    names += [f"coverage-data-adapter-{row['suite']}-py{row['python']}" for row in adapters["include"]]
    names += [f"coverage-data-service-{row['suite']}" for row in services["include"]]
    for suite, selected in (("examples", examples), ("provider-recipes", provider_recipes)):
        if selected:
            names.extend(f"coverage-data-{suite}-py{version}" for version in ("3.11", "3.15"))
    if len(names) != len(set(names)):
        raise ValueError("Duplicate coverage artifact names in the test matrix")
    suites = {row["suite"] for row in adapters["include"]} | {row["extra"] for row in services["include"]}
    if unknown := suites - ADAPTER_PATHS.keys():
        raise ValueError(f"Unknown adapter source scope: {', '.join(sorted(unknown))}")
    return names, suites


def artifact_files(directory: Path, expected: list[str]) -> list[Path]:
    """Reject absent/empty artifacts before combining any available data."""
    files = []
    for name in expected:
        matches = sorted((directory / name).glob(".coverage.*"))
        if not matches or any(not path.is_file() or path.stat().st_size == 0 for path in matches):
            raise ValueError(f"Missing or empty coverage data for planned artifact: {name}")
        files.extend(matches)
    return files


def source_scope(root: Path, suites: set[str]) -> tuple[list[str], str]:
    package = root / "async_batcher"
    if unknown := suites - ADAPTER_PATHS.keys():
        raise ValueError(f"Unknown adapter source scope: {', '.join(sorted(unknown))}")
    if suites == ADAPTER_PATHS.keys():
        sources = set(package.rglob("*.py"))
        label = "Full package: all async_batcher sources"
    else:
        sources = set(package.glob("*.py"))
        for suite in suites:
            adapter_sources = set((package / ADAPTER_PATHS[suite]).rglob("*.py"))
            if not adapter_sources:
                raise ValueError(f"No source files found for selected adapter: {suite}")
            sources.update(adapter_sources)
        if suites & {"keras", "sklearn"}:
            sources.update((package / "ml").glob("*.py"))
        label = "Scoped coverage: core" + (" + " + ", ".join(sorted(suites)) if suites else " only")
    if not sources:
        raise ValueError("No source files found for the coverage scope")
    return sorted(path.relative_to(root).as_posix() for path in sources), label


def combine_and_report(files: list[Path], sources: list[str], output: Path) -> tuple[float, str]:
    # Quality's pure planning tests do not install the test dependency group.
    from coverage import Coverage, CoverageData

    for path in files:
        data = CoverageData(basename=str(path))
        data.read()
        if not data.has_arcs() or not any(data.lines(name) for name in data.measured_files()):
            raise ValueError(f"Coverage artifact has no executed branch coverage: {path}")
    coverage = Coverage(data_file=str(output / ".coverage"), messages=True)
    coverage.erase()
    coverage.combine(data_paths=[str(path) for path in files], strict=True, keep=True)
    coverage.save()
    report = io.StringIO()
    total = coverage.report(morfs=sources, file=report, show_missing=True, precision=2)
    coverage.html_report(morfs=sources, directory=str(output / "html"))
    coverage.xml_report(morfs=sources, outfile=str(output / "coverage.xml"))
    coverage.json_report(morfs=sources, outfile=str(output / "coverage.json"), pretty_print=True)
    return total, report.getvalue()


def selected_flag(name: str) -> bool:
    value = os.environ.get(name, "false")
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


def recipe_sources(root: Path) -> list[str]:
    sources = sorted(path.relative_to(root).as_posix() for path in (root / "examples/recipes").rglob("*.py"))
    if not sources:
        raise ValueError("No recipe source files found")
    return sources


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine every planned CI test job's coverage data")
    parser.add_argument("--data-dir", type=Path, default=Path("coverage-data"))
    parser.add_argument("--output-dir", type=Path, default=Path("coverage-report"))
    parser.add_argument("--fail-under", type=float, default=95)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not 0 <= args.fail_under <= 100:
            raise ValueError("Coverage threshold must be between 0 and 100")
        expected, suites = planned_artifacts(
            *(json.loads(os.environ[name]) for name in ("CORE_MATRIX", "ADAPTER_MATRIX", "SERVICE_MATRIX")),
            examples=selected_flag("HAS_EXAMPLES"),
            provider_recipes=selected_flag("HAS_PROVIDER_RECIPES"),
        )
        files = artifact_files(args.data_dir, expected)
        sources, scope = source_scope(Path.cwd(), suites)
        total, report = combine_and_report(files, sources, args.output_dir)
        summary = (
            f"## Coverage\n\n{scope} ({len(sources)} files).\n\n"
            f"Received all **{len(expected)}** planned test artifacts. "
            f"Combined statements and branches: **{total:.2f}%**; required: **{args.fail_under:g}%**.\n\n"
            f"```text\n{report}```\n"
        )
        recipe_output = args.output_dir / "recipes"
        recipe_output.mkdir(exist_ok=True)
        recipe_total, recipe_report = combine_and_report(files, recipe_sources(Path.cwd()), recipe_output)
        recipe_totals = json.loads((recipe_output / "coverage.json").read_text())["totals"]
        selected = ["core"]
        if selected_flag("HAS_EXAMPLES"):
            selected.append("HTTP")
        if selected_flag("HAS_PROVIDER_RECIPES"):
            selected.append("provider")
        recipe_summary = (
            "## Recipe coverage (separate from the package gate)\n\n"
            f"All examples/recipes Python sources; selected recipe test suites: {', '.join(selected)}. "
            "Unselected suites can leave sources unexecuted. "
            "Child-process crash/replay execution is not instrumented by this report.\n\n"
            f"Statements: **{recipe_totals['percent_statements_covered']:.2f}%**; "
            f"branches: **{recipe_totals['percent_branches_covered']:.2f}%**; "
            f"combined: **{recipe_total:.2f}%**. "
            "This diagnostic report has no percentage gate and does not change the package threshold.\n\n"
            f"```text\n{recipe_report}```\n"
        )
        (recipe_output / "summary.md").write_text(recipe_summary)
        summary += "\n" + recipe_summary
        result = 0 if total >= args.fail_under else 1
    except Exception as error:
        summary = f"## Coverage incomplete\n\n{type(error).__name__}: {error}\n"
        result = 2
    (args.output_dir / "summary.md").write_text(summary)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a") as stream:
            stream.write(summary)
    print(summary)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
