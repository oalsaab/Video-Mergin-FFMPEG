# Rough Outline

# Read Stream with FFprobe & Deserialse with MgSpec Struct
# Read Stream with Async Queue
# For every Stream processed pick out a property to sort on
# Pass the information of the Stream into a heapQ
# Keep Heap sorted
# Write contents of Heap into Temp File
# Read Temp file into FFMPEG & Concat

import asyncio
import json
import shlex
from asyncio import PriorityQueue
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path

COMMAND = shlex.split("ffprobe -v quiet -print_format json -show_format -show_streams")


@dataclass
class Stream:
    stream: dict
    file: Path

    @property
    def streams(self) -> list[dict]:
        return self.stream.get("streams", [{}])

    @property
    def creation(self) -> datetime:
        _tags: dict = next(iter(self.streams), {}).get("tags", {})
        _creation_time = _tags.get("creation_time")

        if _creation_time is None:
            return None

        return datetime.strptime(_creation_time, "%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(order=True)
class Work:
    stream: Stream = field(compare=False)
    creation: datetime


async def producer(queue: PriorityQueue, file: Path):
    cmd = COMMAND + [file]

    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)

    stdout, _ = await process.communicate()

    # Try out Mgspecs here
    deserialised = json.loads(stdout)
    stream = Stream(deserialised, file)

    work = Work(stream, stream.creation)

    await queue.put(work)

    print(f"Putting {file} in Queue!")


async def consumer(queue: PriorityQueue):
    while not queue.empty():
        item: Work = await queue.get()

        # Write contents to file or list

        print(f"Consuming {item.stream.file} --> {item.creation}")

        queue.task_done()


async def main(files: Path):
    queue = asyncio.PriorityQueue()

    producers = [asyncio.create_task(producer(queue, file)) for file in files.iterdir()]
    await asyncio.gather(*producers)

    asyncio.create_task(consumer(queue))

    await queue.join()


asyncio.run(main())
