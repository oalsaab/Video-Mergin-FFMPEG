from asyncio import PriorityQueue
from collections import defaultdict
from pathlib import Path
from typing import AsyncIterator

from .producer import Work


async def consumer(queue: PriorityQueue) -> AsyncIterator[Work]:
    while not queue.empty():
        item: Work = await queue.get()

        yield item
        queue.task_done()


async def partition(consumed: AsyncIterator[Work]) -> dict[str, list]:
    partitioned = defaultdict(list)

    async for item in consumed:
        partitioned[item.key].append(f"file '{item.file}'\n")

    return {k: v for k, v in partitioned.items() if len(v) != 1}


def writer(merge_path: Path, partitions: dict[str, list]) -> list[str]:
    for stream, inputs in partitions.items():
        with open(Path(f"{merge_path}/{stream}.txt"), "w") as file:
            file.writelines(inputs)

    return list(partitions.keys())
