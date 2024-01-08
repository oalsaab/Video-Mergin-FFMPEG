# Rough Outline

# Read Stream with FFprobe & Deserialse with MgSpec Struct
# Read Stream with Async Queue
# For every Stream processed pick out a property to sort on
# Pass the information of the Stream into a heapQ
# Keep Heap sorted
# Write contents of Heap into Temp File
# Read Temp file into FFMPEG & Concat

import asyncio
import shlex
from asyncio import PriorityQueue
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path

import msgspec

from model import Streams

COMMAND = shlex.split("ffprobe -v quiet -print_format json -show_format -show_streams")


@dataclass(order=True)
class Work:
    creation: datetime
    file: Path = field(compare=False)


async def producer(queue: PriorityQueue, file: Path):
    cmd = COMMAND + [file]

    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)

    stdout, _ = await process.communicate()

    deserialised = msgspec.json.decode(stdout, type=Streams)

    work = Work(
        creation=deserialised.format.tags.creation_time,
        file=file,
    )

    await queue.put(work)

    print(f"Putting {file} in Queue!")


async def consumer(queue: PriorityQueue):
    while not queue.empty():
        item: Work = await queue.get()

        # Write contents to file or list

        print(f"Consuming {item.file} --> {item.creation}")

        queue.task_done()


async def main(files: Path):
    queue = asyncio.PriorityQueue()

    producers = [asyncio.create_task(producer(queue, file)) for file in files.iterdir()]
    await asyncio.gather(*producers)

    asyncio.create_task(consumer(queue))

    await queue.join()


asyncio.run(main())
