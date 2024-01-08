import asyncio
import uuid
from pathlib import Path

from consumer import consumer
from merger import writer
from producer import producer


async def main(directory: Path):
    queue = asyncio.PriorityQueue()
    name = uuid.uuid4()

    producers = [
        asyncio.create_task(producer(queue, file)) for file in directory.iterdir()
    ]

    await asyncio.gather(*producers)

    consumed = consumer(queue)

    await writer(directory, name, consumed)

    await queue.join()


asyncio.run(main(Path(r"c:\Users\omar_\Videos\testing\test")))
