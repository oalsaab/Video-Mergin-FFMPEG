from pathlib import Path
from typing import AsyncIterator


async def writer(directory: Path, name: str, consumed: AsyncIterator[str]):
    destination = Path(f"{directory}/{name}.txt")

    with open(destination, "w") as fp:
        async for item in consumed:
            fp.write(item)
