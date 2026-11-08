# Installation

These instructions target Async Batcher **0.3.0**, which requires **Python 3.11 or newer**.
Check the shared [release notes](https://hussein.awala.fr/async-batcher/release-notes/)
for publication status. The documentation banner identifies release, prerelease, and
development pages; use the version matching your package.

## Install the package

In your application's virtual environment:

```bash
python -m pip install 'async-batcher==0.3.0'
```

With a uv-managed application:

```bash
uv add 'async-batcher==0.3.0'
```

The core has no runtime dependencies. Run Python examples with `python demo.py`,
or `uv run python demo.py` in a uv-managed application. You do not need to clone
this repository to use the core API or adapters. See [your first batch](index.md#your-first-batch).

For an upgrade, read [migration from 0.2.2](migration.md), keep the extras your
application needs, and test its configuration, shutdown, and backend error handling.

## Optional adapters

Install only the clients you need; importing the core does not import optional SDKs.
For example:

```bash
python -m pip install 'async-batcher[aws]==0.3.0'
# Or in a uv-managed application:
uv add 'async-batcher[aws]==0.3.0'
```

| Extra | Operations and requirements |
| --- | --- |
| `aws` | [DynamoDB](dynamodb.md), [Kinesis and Firehose](aws-streams.md); configure AWS credentials and region |
| `sqlalchemy` | [SQLAlchemy 2 database writes](databases.md#sqlalchemy); also install your dialect's async driver |
| `scylla` | [ScyllaDB writes](databases.md#scylladb); configure a cqlengine connection |
| `mongodb` | [MongoDB writes](databases.md#mongodb) using an application-owned asynchronous client |
| `kafka` | [Kafka producer batches](kafka.md); configure the broker and caller-owned producer |
| `pinecone` | [Pinecone vector operations](pinecone.md); configure the compatible index and namespace |
| `keras` | [Keras inference](machine-learning.md#keras); install a compatible backend separately |
| `sklearn` | [scikit-learn inference](machine-learning.md#scikit-learn) with a fitted estimator |

Combine extras when needed, for example `'async-batcher[aws,kafka]==0.3.0'`.
Each guide specifies resource ownership, response mapping, and failure behavior.
The repository's test and example dependency groups are contributor tools, not package extras.

## Run the repository recipes

The runnable modules under `examples/recipes/` and the older applications under
`examples/` are repository examples. They are **not installed by the wheel**.
To run them, obtain the source matching the docs, then use its locked environment:

```bash
git clone https://github.com/hussein-awala/async-batcher.git
cd async-batcher
git checkout --detach 0.3.0
uv sync --locked --no-default-groups --python 3.12
```

Published documentation pins this checkout command to its exact source commit.
When reading the Markdown directly on GitHub, choose the published tag or a reviewed
commit. The `0.3.0` tag becomes available when that release is published. A development
preview can use the source commit linked in its banner before the tag exists.

Run a recipe from the repository root, for example:

```bash
uv run --no-sync python -m examples.recipes.sqlite_stream --help
```

Follow the selected guide's additional sync command for HTTP or provider dependencies.
`uv sync` replaces the selected environment; `uv run --no-sync` keeps it unchanged.
Legacy Keras/gRPC applications have separate README instructions and environments.
Core and adapter application code can use the PyPI package without these example files.

## Develop or test the library

Source builds, tests, typing checks, live service setup, and documentation previews
are described in [Contributing](../CONTRIBUTING.md). Those workflows use uv and
`uv.lock`; they are separate from installing the library into an application.
