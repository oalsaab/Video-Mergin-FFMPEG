import asyncio
import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple
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
        Path(self.merge_path).mkdir(parents=True, exist_ok=True)

    @property
    def merge_path(self) -> Path:
        return Path(f"{self.directory}/{self.uuid}")

    def __iter__(self) -> Iterator[Path]:
        for file in self.directory.iterdir():
            if file.is_dir():
                continue

            yield file


class PreProcessed(NamedTuple):
    context: Context
    inputs: list[str]


async def preprocess(directory: Path) -> PreProcessed:
    logging.basicConfig(level=logging.INFO)

    queue = asyncio.PriorityQueue()

    context = Context(directory, uuid.uuid4())

    producers = [asyncio.create_task(producer(queue, file)) for file in context]

    await asyncio.gather(*producers)

    consumed = consumer(queue)

    inputs = await writer(context.merge_path, consumed)

    await queue.join()

    return PreProcessed(context, inputs)


def main():
    directory = Path(r"c:\Users\omar_\Videos\testing\test")

    preprocessed = asyncio.run(preprocess(directory))
