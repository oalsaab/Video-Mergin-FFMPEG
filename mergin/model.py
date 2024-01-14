from collections.abc import Iterator
from datetime import datetime
from datetime import timezone

from msgspec import Struct


class Tags(Struct):
    creation_time: datetime = datetime.fromtimestamp(0, timezone.utc)


class Format(Struct):
    tags: Tags


class Stream(Struct):
    codec_name: str
    codec_type: str
    width: int | None = None
    height: int | None = None


class MultiMedia(Struct):
    format: Format
    streams: list[Stream]

    @property
    def video(self) -> Stream | None:
        for stream in self.streams:
            if stream.codec_type == "video":
                return Stream

        raise None

    def __len__(self) -> int:
        return len(self.streams)

    def __iter__(self) -> Iterator:
        for stream in self.streams:
            yield stream
