import shlex
import subprocess
from pathlib import Path


# You can most likely remove the -safe 0 parameter here,
# Read the docs on concat, add notes in readme that it is a demuxer / muxer.
def merger(directory: Path, file_input: Path, file_output: Path):
    cmd = shlex.split(
        f"ffmpeg "
        f"-hide_banner -loglevel error -f concat -safe 0 "
        f"-i {file_input.name} "
        f"-c copy {file_output.name}"
    )

    subprocess.run(cmd, cwd=directory)
