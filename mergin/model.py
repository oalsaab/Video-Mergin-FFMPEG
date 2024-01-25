from collections.abc import Iterator
from datetime import datetime
from datetime import timezone

import msgspec
from msgspec import Struct


class Tags(Struct):
    creation_time: datetime = datetime.fromtimestamp(0, timezone.utc)


class Format(Struct):
    tags: Tags
    nb_streams: int


class Stream(Struct):
    codec_name: str
    codec_type: str
    width: int
    height: int
    has_b_frames: int | None = None

    def __bool__(self) -> bool:
        return self.codec_type == "video"


class MultiMedia(Struct):
    format: Format
    streams: list[Stream]

    @property
    def stream(self) -> Stream | None:
        return next(iter(self.streams))

    @property
    def is_video(self) -> bool:
        return bool(self.stream)

    @property
    def key(self) -> str:
        audio = "audio" if (self.format.nb_streams) > 1 else "no_audio"

        fields = [str(field) for field in msgspec.structs.astuple(self.stream)]
        return "_".join(fields + [audio])

    def __len__(self) -> int:
        return len(self.streams)

    def __iter__(self) -> Iterator[Stream]:
        for stream in self.streams:
            yield stream
