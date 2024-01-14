import shlex
import subprocess
from functools import partial
from multiprocessing import Pool
from pathlib import Path


def multi_merge(merge_path: Path, inputs: list[str]):
    uniques = len(inputs)
    process = partial(merger, merge_path)

    with Pool(uniques) as pool:
        pool.map(process, inputs)


# Read the docs on concat, add notes in readme that it is a demuxer / muxer.
def merger(merge_path: Path, txt_input: str):
    cmd = shlex.split(
        f"ffmpeg "
        f"-hide_banner -loglevel error -f concat -safe 0 "
        f"-i {txt_input}.txt "
        f"-c copy {txt_input}.mkv"
    )

    subprocess.run(cmd, cwd=merge_path)
