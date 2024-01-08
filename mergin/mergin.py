import asyncio
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple
from uuid import UUID

from consumer import consumer
from merger import merger
from merger import writer
from producer import producer

# TODO:
# Clean up after merging, use context manager with 'finally' statements
# Use something more robust than the Format / Tags keys, look into using Streams keys?
# Look into async errors, propogate errors so script can end early rather than hang in queue.join()
# Swap print statements for log statements
# Add entry point of script to Poetry
# Use Typer or Click to construct some CLI?


class File(NamedTuple):
    directory: Path
    uuid: UUID

    @property
    def _path(self) -> Path:
        return Path(f"{self.directory}/{self.uuid}")

    @property
    def inp(self) -> Path:
        return Path(f"{self._path}.txt")

    @property
    def out(self) -> Path:
        return Path(f"{self._path}.mov")

    def __str__(self):
        return (
            f"Creating FFMPEG input file: {self.inp}\n"
            f"Creating FFMPEG output file: {self.out}"
        )


async def main(directory: Path):
    queue = asyncio.PriorityQueue()

    file = File(directory, uuid.uuid4())
    print(file)

    producers = [
        asyncio.create_task(producer(queue, file)) for file in directory.iterdir()
    ]

    await asyncio.gather(*producers)

    consumed = consumer(queue)

    await writer(file.inp, consumed)

    await queue.join()

    merger(directory, file.inp, file.out)


asyncio.run(main(Path(r"c:\Users\omar_\Videos\testing\test")))
