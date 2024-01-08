import shlex
import subprocess
from pathlib import Path
from typing import AsyncIterator


async def writer(file_input: Path, consumed: AsyncIterator[str]):
    with open(file_input, "w") as fp:
        async for item in consumed:
            fp.write(item)


def merger(directory: Path, file_input: Path, file_output: Path):
    cmd = shlex.split(
        f"ffmpeg "
        f"-hide_banner -loglevel error -f concat -safe 0 "
        f"-i {file_input.name} "
        f"-c copy {file_output.name}"
    )

    subprocess.run(cmd, cwd=directory)
