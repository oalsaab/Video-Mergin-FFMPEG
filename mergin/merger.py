from pathlib import Path
from typing import AsyncIterator


async def writer(file_input: Path, consumed: AsyncIterator[str]):
    with open(file_input, "w") as fp:
        async for item in consumed:
            fp.write(item)
