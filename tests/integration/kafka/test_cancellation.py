from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("aiokafka")

from aiokafka import AIOKafkaProducer  # noqa: E402
from aiokafka.structs import RecordMetadata  # noqa: E402
from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord  # noqa: E402

from tests.integration.kafka.test_producer import BOOTSTRAP, consume, topic_with_config  # noqa: E402

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.integration_kafka]


@pytest.mark.parametrize("force_stop", [False, True], ids=["caller-cancellation", "force-stop"])
async def test_cancel_after_real_enqueue_preserves_delivery_and_producer_ownership(monkeypatch, force_stop):
    async with topic_with_config() as topic:
        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, request_timeout_ms=10000)
        batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=2)
        enqueued = asyncio.Event()
        release = asyncio.Event()
        deliveries = []
        requests = []
        native_send_batch = producer.send_batch

        async def gated_send_batch(builder, topic, *, partition):
            # Keep the actual SDK enqueue and broker delivery. The gate only
            # controls when the adapter can receive the producer-owned future.
            delivery = await native_send_batch(builder, topic, partition=partition)
            deliveries.append(delivery)
            enqueued.set()
            await release.wait()
            return delivery

        monkeypatch.setattr(producer, "send_batch", gated_send_batch)
        try:
            async with asyncio.timeout(30):
                await producer.start()
                requests = [
                    asyncio.create_task(batcher.process(KafkaRecord(topic=topic, partition=0, value=value)))
                    for value in (b"first", b"second")
                ]
                await enqueued.wait()
                assert len(deliveries) == 1
                if force_stop:
                    await batcher.stop(force=True, timeout=10)
                    outcomes = await asyncio.gather(*requests, return_exceptions=True)
                    assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
                else:
                    requests[0].cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await requests[0]
                    assert not requests[1].done()
                assert not deliveries[0].cancelled()
                release.set()
                native_result = await asyncio.shield(deliveries[0])
                assert isinstance(native_result, RecordMetadata)
                if not force_stop:
                    peer = await requests[1]
                    assert peer.offset == native_result.offset + 1
                await batcher.stop(timeout=10)
                owner_result = await producer.send_and_wait(topic, b"owner", partition=0)
                assert owner_result.offset == native_result.offset + 2
            records = await consume(topic, 3)
            assert [record.value for record in records] == [b"first", b"second", b"owner"]
            assert [record.offset for record in records] == list(
                range(native_result.offset, owner_result.offset + 1)
            )
        finally:
            release.set()
            await batcher.stop(force=True, timeout=10)
            await asyncio.gather(*requests, return_exceptions=True)
            await producer.stop()
