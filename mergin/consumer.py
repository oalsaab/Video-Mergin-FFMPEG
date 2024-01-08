from asyncio import PriorityQueue
from typing import AsyncIterator

from producer import Work


async def consumer(queue: PriorityQueue) -> AsyncIterator[str]:
    while not queue.empty():
        item: Work = await queue.get()

        yield f"file '{item.file.name}'\n"
        queue.task_done()
