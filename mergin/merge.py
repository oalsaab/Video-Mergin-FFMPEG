import asyncio
import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple
from uuid import UUID

import click

from .consumer import consumer
from .consumer import partition
from .consumer import writer
from .merger import finalise
from .merger import merger
from .producer import producer


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

    def __str__(self) -> str:
        return f"Created directory to store results: {self.merge_path.name}"


class PreProcessed(NamedTuple):
    context: Context
    inputs: list[str]


async def preprocess(directory: Path) -> PreProcessed:
    queue = asyncio.PriorityQueue()

    context = Context(directory, uuid.uuid4())
    logging.info(context)

    producers = [asyncio.create_task(producer(queue, file)) for file in context]

    await asyncio.gather(*producers)
    consumed = consumer(queue)

    partitions = await partition(consumed)
    await queue.join()

    inputs = writer(context.merge_path, partitions)
    return PreProcessed(context, inputs)


@click.command()
@click.argument("directory", type=click.Path(exists=True, path_type=Path))
def mergin(directory: Path):
    logging.basicConfig(level=logging.INFO)

    logging.info("Initiating Preprocessing...")
    preprocessed = asyncio.run(preprocess(directory))

    logging.info("Finished Preprocessing...")

    merged = merger(preprocessed.context.merge_path, preprocessed.inputs)
    finalise(preprocessed.context.merge_path, merged)

    logging.info("Completed Merges")
