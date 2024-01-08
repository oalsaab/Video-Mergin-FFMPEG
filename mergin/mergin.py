import asyncio
from pathlib import Path

from consumer import consumer
from producer import producer


async def main(files: Path):
    queue = asyncio.PriorityQueue()

    producers = [asyncio.create_task(producer(queue, file)) for file in files.iterdir()]
    await asyncio.gather(*producers)

    asyncio.create_task(consumer(queue))

    await queue.join()


asyncio.run(main(Path(r"c:\Users\omar_\Videos\testing\test")))
