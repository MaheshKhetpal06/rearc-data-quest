"""Discover files from a BLS directory page. No filename is assumed in advance."""

from __future__ import annotations

import re
from dataclasses import dataclass

_ROW = re.compile(
    r"(?P<modified>\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)\s+"
    r"(?P<size>\d+)\s+"
    r'<A\s+HREF="[^"]*">(?P<name>[^<]+)</A>',
    re.IGNORECASE,
)


class ListingError(RuntimeError):
    pass


@dataclass(frozen=True)
class ListedFile:
    name: str
    modified: str
    size: int


def parse_listing(html: str) -> dict[str, ListedFile]:
    """Parse a directory page. Finding no files is an error, so a bad page can't look like deletions."""
    files = {
        match["name"]: ListedFile(
            name=match["name"], modified=" ".join(match["modified"].split()), size=int(match["size"])
        )
        for match in _ROW.finditer(html)
    }
    if not files:
        raise ListingError("No files found in the directory listing; refusing to continue.")
    return files
