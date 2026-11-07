# Security Policy

## Supported versions

This project is in development. Maintainers address security issues in a patch release or in
the next minor release when it also includes new features. The repository's `main` branch can
contain fixes that have not yet been published. Include the installed package version or source
commit when reporting a problem; do not assume a source checkout and the latest release are identical.

## Reporting a vulnerability

Please report vulnerabilities privately through a
[GitHub security advisory](https://github.com/hussein-awala/async-batcher/security/advisories/new).
Do not include exploit details, credentials, or affected user data in a public issue.
Include the affected version/commit, relevant adapter and dependency versions, reproduction steps,
and expected impact. Maintainers will review the report and coordinate remediation.

## Dependency checks

The repository audits every registry package version in `uv.lock` against OSV, including conditional
versions and optional extras. See [the audit workflow and exception policy](CONTRIBUTING.md#locked-dependency-security-audit).
This checks known dependency advisories; it does not prove the absence of vulnerabilities in the
library, application, optional service, example environment, or container image.
