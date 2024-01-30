from asyncio import Queue
from collections import defaultdict
from heapq import heappop
from heapq import heappush
from pathlib import Path
from typing import TypeAlias

from .producer import Work

Partitioned: TypeAlias = dict[str, list[Work]]


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
    partitioned = defaultdict(list)

    while consumed:
        item = heappop(consumed)
        partitioned[item.key].append(item)

    return {k: v for k, v in partitioned.items() if len(v) != 1}


def writer(merge_path: Path, partitions: Partitioned) -> list[str]:
    for stream, inputs in partitions.items():
        with open(Path(f"{merge_path}/{stream}.txt"), "w") as file:
            file.writelines((f"file '{line.file}'\n" for line in inputs))

    return list(partitions.keys())
