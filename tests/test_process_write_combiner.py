from __future__ import annotations

import asyncio
from types import SimpleNamespace

from aiokafka import TopicPartition

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer


class _CombinerConsumer(BaseKafkaConsumer):
    def __init__(self) -> None:
        # The unit test exercises only the process-level combiner and therefore
        # supplies a non-owned DB sentinel instead of opening external services.
        super().__init__(topic="raw", group_id="cg-combiner-test", db=object())
        self.calls: list[list[tuple[int, int]]] = []

    async def process_batch(self, records):
        self.calls.append([(record.partition, record.offset) for record in records])
        return None


def _record(partition: int, offset: int):
    return SimpleNamespace(
        partition=partition,
        offset=offset,
        value={},
    )


def test_combiner_coalesces_partitions_and_preserves_partition_fifo() -> None:
    async def scenario() -> None:
        consumer = _CombinerConsumer()
        consumer._process_combiner_task = asyncio.create_task(
            consumer._process_combiner_loop()
        )
        first = asyncio.create_task(consumer._process_partition_batch(
            TopicPartition("raw", 0), [_record(0, 10), _record(0, 11)]
        ))
        second = asyncio.create_task(consumer._process_partition_batch(
            TopicPartition("raw", 1), [_record(1, 20), _record(1, 21)]
        ))
        assert await asyncio.gather(first, second) == [None, None]

        await consumer._process_queue.put(None)
        await consumer._process_combiner_task

        assert len(consumer.calls) == 1
        combined = consumer.calls[0]
        assert [offset for partition, offset in combined if partition == 0] == [10, 11]
        assert [offset for partition, offset in combined if partition == 1] == [20, 21]

    asyncio.run(scenario())
