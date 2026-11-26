#!/usr/bin/env python3
"""Build tests/golden/sample.log: a few hundred lines copied UNCHANGED from the real NASA files.

Lines are only selected, never edited (the file is read and written as bytes). The selection
is meant to cover every case in SPEC.md section 2.4 and to make Q1-Q3 non-trivial:
  * the first lines of each file, and lines spread across the whole file (many dates and hours);
  * a few lines of every status code, bytes '-', no protocol, empty request, one-token request,
    other protocols (HTTP/V1.0, HTTP/*), spaces in the path, non-ASCII bytes, malformed lines;
  * lines for the most popular paths (ties and repeated hosts for Q2's top 20 and distinct hosts);
  * July's cut-off last line (`alyssa.p`), written LAST and without a newline, as in the real file.

The line pattern below is used only to CHOOSE lines for this test file. It plays no part in any
pipeline's results; the expected results come from tests/run_reference.py.

usage (from the project root):
    python3 pipelines/mapreduce/make_golden_sample.py
    python3 pipelines/mapreduce/make_golden_sample.py --data data --out tests/golden/sample.log
"""
import argparse
import collections
import re
from pathlib import Path

LINE = re.compile(
    r'^(\S+) \S+ \S+ \[((0[1-9]|[12]\d|3[01])/'
    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\d{4}):([01]\d|2[0-3]):[0-5]\d:[0-5]\d [+-]\d{4})\] '
    r'"(.*)" (\d{3}) (\d+|-)$',
    re.ASCII,
)
REQUEST = re.compile(r"^(\S+)\s+(.+?)(?:\s+(HTTP/\S+))?$", re.ASCII)

FILES = ["NASA_access_log_Jul95", "NASA_access_log_Aug95"]  # July first, then August (SPEC section 1)
POPULAR = ["/images/NASA-logosmall.gif", "/images/KSC-logosmall.gif", "/images/MOSAIC-logosmall.gif"]
REQUIRED = ["malformed", "dash_bytes", "no_protocol", "empty_request", "one_token_request",
            "odd_protocol", "space_in_path", "non_ascii"]

FIRST_LINES = 40      # per file
SPREAD = 60           # about this many evenly spaced lines per file
CAP_STATUS = 4        # lines per status code, per file
CAP_SPECIAL = 6       # lines per special case, per file
CAP_POPULAR = 8       # lines per popular path, per file
EARLY_WINDOW = 5000   # window for the repeated-host lines


def tags(raw):
    """Why a line is interesting: a list of tag names."""
    line = raw.decode("latin-1")
    m = LINE.fullmatch(line)
    if m is None:
        return ["malformed"], None, None
    host, _ts, _d, _mo, _y, _h, request, status, nbytes = m.groups()
    t = ["status_" + status]
    if nbytes == "-":
        t.append("dash_bytes")
    request = request.strip(" ")
    r = REQUEST.fullmatch(request)
    path = None
    if r is None:
        t.append("empty_request" if request == "" else "one_token_request")
    else:
        path = r.group(2)
        if not r.group(3):
            t.append("no_protocol")
        elif r.group(3) != "HTTP/1.0":
            t.append("odd_protocol")
        if " " in path:
            t.append("space_in_path")
    if any(b >= 0x80 for b in raw):
        t.append("non_ascii")
    return t, host, path


def pick_from_file(path):
    """Return (chosen line indexes, lines, cut-off last line or None)."""
    data = path.read_bytes()
    lines = data.split(b"\n")
    cutoff = None
    if data.endswith(b"\n"):
        lines.pop()                      # nothing after the last newline
    else:
        cutoff = lines.pop()             # last line has no newline: the cut-off line
    n = len(lines)

    chosen = set(range(min(FIRST_LINES, n)))
    chosen.update(range(0, n, max(1, n // SPREAD)))

    taken = collections.Counter()
    early_hosts = []
    for i, raw in enumerate(lines):
        t, host, p = tags(raw)
        if i < EARLY_WINDOW:
            early_hosts.append(host)
        for tag in t:
            cap = CAP_STATUS if tag.startswith("status_") else CAP_SPECIAL
            if taken[tag] < cap:
                taken[tag] += 1
                chosen.add(i)
        if p in POPULAR and taken["popular:" + p] < CAP_POPULAR:
            taken["popular:" + p] += 1
            chosen.add(i)

    # a host that shows up repeatedly early in the file, so Q2 has repeated hosts on one path
    counts = collections.Counter(h for h in early_hosts if h)
    if counts:
        top_host = counts.most_common(1)[0][0]
        chosen.update([j for j, h in enumerate(early_hosts) if h == top_host][:CAP_POPULAR])
    return sorted(chosen), lines, cutoff


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="data", help="folder with the two decompressed NASA files")
    ap.add_argument("--out", default="tests/golden/sample.log")
    args = ap.parse_args()

    out_lines, cutoff = [], None
    for name in FILES:
        path = Path(args.data) / name
        if not path.is_file():
            raise SystemExit(f"missing {path}: run scripts/download_data.sh first")
        idx, lines, cut = pick_from_file(path)
        out_lines += [lines[i] for i in idx]
        print(f"{name}: {len(lines):,} lines, picked {len(idx)}")
        if cut is not None:
            cutoff = cut

    body = b"\n".join(out_lines)
    if cutoff is not None:
        body += b"\n" + cutoff            # last line, no newline, exactly as in the July file
    else:
        body += b"\n"
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(body)

    # report what the sample covers
    seen = collections.Counter()
    for raw in out_lines + ([cutoff] if cutoff is not None else []):
        for tag in tags(raw)[0]:
            seen[tag] += 1
    total = len(out_lines) + (1 if cutoff is not None else 0)
    print(f"\nwrote {out}: {total} lines ({'ends without newline, like July' if cutoff is not None else 'ends with newline'})")
    for tag in sorted(seen):
        print(f"  {tag:20s} {seen[tag]}")
    missing = [t for t in REQUIRED if seen[t] == 0]
    if missing:
        print("\nWARNING: no real line found for: " + ", ".join(missing)
              + "\n(lines are never invented; tell the team which SPEC 2.4 cases the real data does not contain)")


if __name__ == "__main__":
    main()
