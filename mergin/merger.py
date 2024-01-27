import logging
import shlex
import subprocess
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple

from .model import Stream


class Result(NamedTuple):
    code: int
    inp: str

    def create_header(self) -> str:
        values = self.inp.split("_")
        fields = [*Stream.__struct_fields__, *["audio"]]

        mapped = dict(zip(fields, values))
        audio = "Y" if mapped.get("audio") == "audio" else "N"
        mapped.update({"audio": audio})

        info = (f"{k.upper()}: {v}" for k, v in mapped.items())
        header = " || ".join(info)
        border = "=" * (len(header) + 4)

        return f"\n{border}\n| {header} |\n{border}\n"

    def __str__(self) -> str:
        status = "Successful" if bool(self) else "Failed"
        return f"{status}: Merge of {self.inp}"

    def __bool__(self) -> bool:
        return self.code == 0


def merger(merge_path: Path, inputs: list[str]):
    logging.info("Initiating Merges...")
    uniques = len(inputs)
    process = partial(_merge, merge_path)

    with Pool(uniques) as pool:
        for result in pool.imap_unordered(process, inputs):
            logging.info(result)
            logging.info(result.create_header())
            pass


# Read the docs on concat, add notes in readme that it is a demuxer / muxer.
def _merge(merge_path: Path, txt_input: str) -> Result:
    cmd = shlex.split(
        f"ffmpeg "
        f"-hide_banner "
        f"-loglevel error "
        f"-f concat "
        f"-safe 0 "
        f"-i {txt_input}.txt "
        f"-c copy {txt_input}.mkv"
    )

    process = subprocess.run(cmd, cwd=merge_path)
    return Result(process.returncode, txt_input)
