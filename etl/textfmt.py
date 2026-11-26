"""Plain-text tables for the terminal, with no extra dependencies."""
from __future__ import annotations

from datetime import date, datetime


def fmt_value(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    return text if text != "" else '""'


def table(headers: list[str], rows: list[tuple], indent: str = "  ") -> str:
    cells = [[fmt_value(v) for v in row] for row in rows]
    numeric = [
        all(isinstance(row[i], (int, float)) and not isinstance(row[i], bool) for row in rows) and bool(rows)
        for i in range(len(headers))
    ]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]

    def line(values):
        parts = [v.rjust(w) if num else v.ljust(w) for v, w, num in zip(values, widths, numeric)]
        return indent + "  ".join(parts).rstrip()

    out = [line(headers), indent + "  ".join("-" * w for w in widths)]
    out += [line(r) for r in cells]
    return "\n".join(out)


def key_values(pairs: list[tuple[str, object]], indent: str = "  ") -> str:
    width = max(len(k) for k, _ in pairs)
    return "\n".join(f"{indent}{k.ljust(width)}  {v if isinstance(v, str) else fmt_value(v)}"
                     for k, v in pairs)
