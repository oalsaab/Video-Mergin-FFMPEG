import asyncio
import logging
import shlex
from asyncio import Queue
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path

import msgspec
from msgspec import ValidationError

from .model import MultiMedia

COMMAND = shlex.split(
    f"ffprobe "
    f"-v quiet "
    f"-print_format json "
    f"-show_format "
    f"-show_streams "
    f"-select_streams v:0"
)


@dataclass(order=True)
class Work:
    creation: datetime
    key: str = field(compare=False)
    file: Path = field(compare=False)


def _multimedia_decode(stdout: bytes, file: Path) -> MultiMedia | None:
    try:
        deserialised = msgspec.json.decode(stdout, type=MultiMedia)
    except ValidationError:
        logging.warning("Failed to decode: %s", file.name)
        return None

    if deserialised.is_video is False:
        logging.warning("Failed to identify video stream for: %s", file.name)
        return None

    return deserialised


async def producer(queue: Queue, files: list[Path]):
    for file in files:
        cmd = COMMAND + [file]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE
        )

        stdout, _ = await process.communicate()
        multimedia = _multimedia_decode(stdout, file)

        if multimedia is not None:
            work = Work(
                creation=multimedia.format.tags.creation_time,
                key=multimedia.key,
                file=file,
            )

            await queue.put(work)

    await queue.put(None)
