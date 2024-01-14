import asyncio
import logging
import shlex
from asyncio import PriorityQueue
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path

import msgspec
from model import MultiMedia
from msgspec import ValidationError

COMMAND = shlex.split("ffprobe -v quiet -print_format json -show_format -show_streams")


@dataclass(order=True)
class Work:
    creation: datetime
    file: Path = field(compare=False)


def _multimedia_decode(stdout: bytes, file: Path) -> MultiMedia | None:
    try:
        deserialised = msgspec.json.decode(stdout, type=MultiMedia)
    except ValidationError:
        logging.warning("Failed to decode: %s", file.name)
        return None

    if deserialised.video is None:
        logging.warning("Failed to identify video stream for: %s", file.name)
        return None

    return deserialised


async def producer(queue: PriorityQueue, file: Path):
    cmd = COMMAND + [file]

    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)

    stdout, _ = await process.communicate()

    multimedia = _multimedia_decode(stdout, file)

    if multimedia is not None:
        work = Work(
            creation=multimedia.format.tags.creation_time,
            file=file,
        )

        await queue.put(work)
