from __future__ import annotations

import asyncio
import sys
import time


async def probe(service: str) -> None:
    if service == "aws":
        import aioboto3

        session = aioboto3.Session(
            region_name="us-west-2", aws_access_key_id="local", aws_secret_access_key="local"
        )
        async with session.client("dynamodb", endpoint_url="http://localhost:8000") as client:
            await client.list_tables(Limit=1)
    elif service == "sqlalchemy":
        import asyncpg

        connection = await asyncpg.connect("postgresql://postgres:password@localhost/postgres", timeout=5)
        try:
            await connection.execute("SELECT 1")
        finally:
            await connection.close()
    elif service == "scylladb":
        from cassandra.cluster import Cluster

        # Gossip health alone does not guarantee that CQL is ready.
        cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=5, control_connection_timeout=5)
        try:
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local", timeout=5)
        finally:
            cluster.shutdown()
    else:
        raise ValueError(f"Unknown service: {service}")


async def wait(service: str) -> None:
    deadline = time.monotonic() + 120
    while True:
        try:
            await asyncio.wait_for(probe(service), timeout=15)
        except Exception as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{service} did not become ready within 120 seconds") from error
            print(f"Waiting for {service}: {type(error).__name__}: {error}", flush=True)
            await asyncio.sleep(2)
        else:
            print(f"{service} is ready", flush=True)
            return


if __name__ == "__main__":
    asyncio.run(wait(sys.argv[1]))
