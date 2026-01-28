from __future__ import annotations

import os
from typing import Optional

try:
    from rich.console import Console
    RICH_AVAILABLE = True
except Exception:
    RICH_AVAILABLE = False


_console: Optional[Console] = Console() if RICH_AVAILABLE else None
_dev_mode: bool = False


def _plain(prefix: str, message: str) -> None:
    print(f"{prefix} {message}")


def info(prefix: str, message: str) -> None:
    if not _dev_mode:
        return
    if RICH_AVAILABLE:
        _console.print(f"[bold cyan]{prefix}[/] {message}")
    else:
        _plain(prefix, message)


def success(prefix: str, message: str) -> None:
    if not _dev_mode:
        return
    if RICH_AVAILABLE:
        _console.print(f"[bold green]{prefix}[/] {message}")
    else:
        _plain(prefix, message)


def warn(prefix: str, message: str) -> None:
    if not _dev_mode:
        return
    if RICH_AVAILABLE:
        _console.print(f"[bold yellow]{prefix}[/] {message}")
    else:
        _plain(prefix, message)


def error(prefix: str, message: str) -> None:
    if RICH_AVAILABLE:
        _console.print(f"[bold red]{prefix}[/] {message}")
    else:
        _plain(prefix, message)


def section(message: str) -> None:
    if RICH_AVAILABLE:
        _console.print(f"[bold magenta]{message}[/]")
    else:
        print(message)

def summary(prefix: str, message: str) -> None:
    if RICH_AVAILABLE:
        _console.print(f"[bold white]{prefix}[/] {message}")
    else:
        _plain(prefix, message)


def set_dev_mode(value: bool) -> None:
    global _dev_mode
    _dev_mode = value


def is_dev_mode() -> bool:
    return _dev_mode
