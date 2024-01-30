from asyncio import Queue
from collections import defaultdict
from heapq import heappop
from heapq import heappush
from pathlib import Path
from typing import TypeAlias

from .producer import Work

Partitioned: TypeAlias = dict[Path, list[Work]]


async def consumer(queue: Queue) -> list[Work]:
    ordered = []

    while True:
        item: Work = await queue.get()
        queue.task_done()

        if item is None:
            break

        heappush(ordered, item)

    return ordered


async def partition(consumed: list[Work]) -> Partitioned:
    partitions = defaultdict(list)

    while consumed:
        item = heappop(consumed)
        partition = Path(f"{item.key}.txt")

        partitions[partition].append(item)

    return {k: v for k, v in partitions.items() if len(v) != 1}


def writer(merge_path: Path, partitions: Partitioned) -> list[Path]:
    for partition, inputs in partitions.items():
        with open(merge_path / partition, "w") as file:
            file.writelines((f"file '{line.file}'\n" for line in inputs))

    return list(partitions.keys())
