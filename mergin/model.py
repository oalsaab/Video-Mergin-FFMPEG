from datetime import datetime

from msgspec import Struct


class Tags(Struct):
    creation_time: datetime


class Format(Struct):
    tags: Tags


class Streams(Struct):
    format: Format
