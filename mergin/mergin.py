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
from merger import multi_merge
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

    def __str__(self) -> str:
        return f"Created directory to store results: {self.merge_path}"


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

    inputs = await writer(context.merge_path, consumed)
    await queue.join()

    return PreProcessed(context, inputs)


def main():
    logging.basicConfig(level=logging.INFO)
    directory = Path(r"c:\Users\omar_\Videos\a_test")

    preprocessed = asyncio.run(preprocess(directory))
    logging.info("Finished Preprocessing...")
    multi_merge(preprocessed.context.merge_path, preprocessed.inputs)


if __name__ == "__main__":
    main()
