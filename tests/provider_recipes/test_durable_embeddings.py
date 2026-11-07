from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from examples.recipes.durable_embeddings import Identity, Spool, deliver_pending, stage_file
from examples.recipes.support import Document
from tests.provider_recipes import test_cohere_pinecone as provider_helpers

providers = provider_helpers.providers


@pytest.mark.asyncio
async def test_local_real_provider_clients_replay_lost_write_ack_without_reembedding(tmp_path, providers):
    path = tmp_path / "input.jsonl"
    path.write_text("".join(json.dumps({"id": str(i), "text": f"document {i}"}) + "\n" for i in range(3)))
    identity = Identity(
        "cohere", "explicit-test-v4", "pinned-revision", "identity", "search_document", 3, "test-index/tenant"
    )
    spool = Spool(tmp_path / "spool.sqlite", "job", identity)
    async with provider_helpers.clients(providers) as (client, index):
        embedder = provider_helpers.CohereEmbedder(client, model=identity.model, dimension=identity.dimension)

        class Model:
            async def embed(self, texts):
                return [
                    vector.values
                    for vector in await embedder.embed(
                        [Document(str(i), text) for i, text in enumerate(texts)]
                    )
                ]

        class Sink:
            lose_ack = True

            async def upsert(self, records):
                await index.upsert(
                    vectors=[provider_helpers.sdk.Vector(id=r.id, values=r.values) for r in records],
                    namespace="tenant",
                )
                if self.lose_ack:
                    raise ConnectionError("Caller lost acknowledgement after provider write")
                return [record.id for record in records]

        model, sink = Model(), Sink()
        with ThreadPoolExecutor(max_workers=2) as pool:
            await stage_file(path, spool, model, executor=pool)
            with pytest.raises(ConnectionError):
                await deliver_pending(spool, sink, executor=pool)
            first_payload = [r[2] for r in providers["requests"] if r[1] == "/vectors/upsert"][0]
            assert len(providers["stored"]) == 3
            assert spool.counts()["pending_vectors"] == 3
            sink.lose_ack = False
            await stage_file(path, spool, model, executor=pool)
            await deliver_pending(spool, sink, executor=pool)
        upserts = [r[2] for r in providers["requests"] if r[1] == "/vectors/upsert"]
        assert upserts == [first_payload, first_payload]
        assert sum(r[1] == "/v2/embed" for r in providers["requests"]) == 1
        assert spool.counts()["pending_vectors"] == 0
        assert not client.is_closed
