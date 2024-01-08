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
