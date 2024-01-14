from asyncio import PriorityQueue
from collections import defaultdict
from pathlib import Path
from typing import AsyncIterator

from producer import Work


async def consumer(queue: PriorityQueue) -> AsyncIterator[Work]:
    while not queue.empty():
        item: Work = await queue.get()

        yield item
        queue.task_done()


async def writer(directory: Path, consumed: AsyncIterator[Work]) -> list[str]:
    streams = defaultdict(list)

    async for item in consumed:
        streams[item.key].append(f"file '{item.file.name}'\n")

    for stream, inputs in streams.items():
        with open(Path(f"{directory}/{stream}.txt"), "w") as file:
            file.writelines(inputs)

    return list(streams.keys())
