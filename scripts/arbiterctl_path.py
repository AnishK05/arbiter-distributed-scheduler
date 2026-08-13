"""Resolve host-side arbiterctl path on Windows and Unix."""

from __future__ import annotations

import os
from pathlib import Path


def resolve_arbiterctl(explicit: str | None = None) -> str:
    """Return a path to arbiterctl / arbiterctl.exe.

    Prefer an explicit CLI flag when provided. Otherwise look under ./bin,
    accepting the Windows .exe suffix so PowerShell builds work with the
    same Python tooling as WSL/Linux/macOS.
    """
    if explicit:
        return explicit
    root = Path(__file__).resolve().parent.parent
    candidates = [
        root / "bin" / "arbiterctl.exe",
        root / "bin" / "arbiterctl",
        Path("bin") / "arbiterctl.exe",
        Path("bin") / "arbiterctl",
        Path("./bin/arbiterctl.exe"),
        Path("./bin/arbiterctl"),
    ]
    for path in candidates:
        if path.is_file():
            return str(path)
    name = "arbiterctl.exe" if os.name == "nt" else "arbiterctl"
    return str(root / "bin" / name)
