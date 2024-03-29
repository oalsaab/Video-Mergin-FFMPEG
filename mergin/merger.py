import logging
import shlex
import subprocess
from contextlib import contextmanager
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Iterator
from typing import NamedTuple
from typing import TextIO

from .model import Stream

MERGE_FILE = Path("merged.txt")


class Result(NamedTuple):
    code: int
    processed: Path

    def create_header(self) -> str:
        values = self.processed.stem.split("_")
        fields = [*Stream.__struct_fields__, *["audio"]]

        mapped = dict(zip(fields, values))
        audio = "Y" if mapped.get("audio") == "audio" else "N"
        mapped.update({"audio": audio})

        info = (f"{k.upper()}: {v}" for k, v in mapped.items())
        header = " || ".join(info)
        border = "=" * (len(header) + 4)

        return f"{border}\n| {header} |\n{border}\n"

    def __str__(self) -> str:
        status = "Successful" if bool(self) else "Failed"
        return f"{status}: Merge of {self.processed.stem}"

    def __bool__(self) -> bool:
        return self.code == 0


def merger(merge_path: Path, inputs: list[Path]) -> Iterator[Result]:
    logging.info("Initiating Merges...")
    uniques = len(inputs)
    process = partial(_concat, merge_path)

    with Pool(uniques) as pool:
        for result in pool.imap_unordered(process, inputs):
            logging.info(result)
            yield result


def _concat(merge_path: Path, txt_input: Path) -> Result:
    cmd = shlex.split(
        f"ffmpeg "
        f"-hide_banner "
        f"-loglevel error "
        f"-f concat "
        f"-safe 0 "
        f"-i {txt_input} "
        f"-c copy {txt_input.stem}.mkv"
    )

    process = subprocess.run(cmd, cwd=merge_path)
    return Result(process.returncode, txt_input)


@contextmanager
def cleanup(path: Path, mode: str) -> Iterator[TextIO]:
    file = open(path, mode)

    try:
        yield file
    finally:
        file.close()
        path.unlink()


def _parse(lines: list[str]) -> Iterator[str]:
    quotes = slice(1, -1)

    for line in lines:
        _, stream = line.split(sep=" ", maxsplit=1)
        sanitised = stream.strip()

        yield f"{Path(sanitised[quotes]).name}\n"


# Over-engineered and over-complicated logic, could just as easily
# pass the results of partition method to simplify things but this
# approach was chosen instead to play around with custom context managers
def finalise(merge_path: Path, results: Iterator[Result]):
    with open(merge_path / MERGE_FILE, "w") as outfile:
        for result in results:
            with cleanup(merge_path / result.processed, "r") as infile:
                header = result.create_header()
                outfile.write(header)

                streams = _parse(infile.readlines())
                outfile.writelines(streams)
