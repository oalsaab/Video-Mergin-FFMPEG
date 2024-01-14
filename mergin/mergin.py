import asyncio
import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from consumer import consumer
from consumer import writer
from merger import merger
from producer import producer

# TODO:
# Clean up after merging, use context manager with 'finally' statements
# Use something more robust than the Format / Tags keys, look into using Streams keys?
# Look into async errors, propogate errors so script can end early rather than hang in queue.join()
# Swap print statements for log statements
# Add entry point of script to Poetry
# Use Typer or Click to construct some CLI?


@dataclass
class Context:
    directory: Path
    uuid: UUID

    def __post_init__(self):
        Path(self.path).mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return Path(f"{self.directory}/{self.uuid}")

    @property
    def out(self) -> Path:
        return Path(f"{self.path}/{self.uuid}.mkv")

    def __iter__(self) -> Iterator[Path]:
        for file in self.directory.iterdir():
            if file.is_dir():
                continue

            yield file

    def __str__(self) -> str:
        return f"Creating FFMPEG output file: {self.out}"


async def main(directory: Path):
    logging.basicConfig(level=logging.INFO)

    queue = asyncio.PriorityQueue()

    context = Context(directory, uuid.uuid4())

    producers = [asyncio.create_task(producer(queue, file)) for file in context]

    await asyncio.gather(*producers)

    consumed = consumer(queue)

    ffmpeg_input = await writer(context.path, consumed)

    await queue.join()

    # merger(directory, file.inp, file.out)


asyncio.run(main(Path(r"c:\Users\omar_\Videos\testing\test")))
