import asyncio
import logging
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import islice
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

QUEUE_LIMIT = 10
SLICE_LIMIT = 50


@dataclass
class Context:
    directory: Path
    uuid: UUID

    def __post_init__(self):
        Path(self.merge_path).mkdir(parents=True, exist_ok=True)

    @property
    def merge_path(self) -> Path:
        return Path(f"{self.directory}/{self.uuid}")

    def __iter__(self) -> Iterator[tuple[Path]]:
        # Implementation of: https://docs.python.org/3/library/itertools.html#itertools.batched
        it = self.directory.iterdir()

        while batch := tuple(islice(it, SLICE_LIMIT)):
            yield batch

    def __str__(self) -> str:
        return f"Created directory to store results: {self.merge_path.name}"


class PreProcessed(NamedTuple):
    context: Context
    inputs: list[Path]


async def preprocess(directory: Path) -> PreProcessed:
    queue = asyncio.Queue(QUEUE_LIMIT)

    context = Context(directory, uuid.uuid4())
    logging.info(context)

    producers = [asyncio.create_task(producer(queue, batch)) for batch in context]
    consumers = asyncio.create_task(consumer(queue))

    await asyncio.gather(*producers)

    # Async queue termination is currently limited: https://discuss.python.org/t/queue-termination/18386
    await queue.join()
    await queue.put(None)

    consumed = await consumers
    partitions = await partition(consumed)

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
