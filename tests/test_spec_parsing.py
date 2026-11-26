"""The examples from SPEC.md section 2.4, checked against the reference parser."""
import pytest

from reference_pipeline import parse_line

PARSED = [
    (b'host-a.example.com - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245\n',
     ("1995-07-01", 0, "GET", "/history/apollo/", "HTTP/1.0", 200, 6245)),
    (b'host-b.example.com - - [01/Jul/1995:13:05:12 -0400] "GET /images/logo.gif HTTP/1.0" 304 -\n',
     ("1995-07-01", 13, "GET", "/images/logo.gif", "HTTP/1.0", 304, 0)),
    (b'host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html" 200 1024\n',
     ("1995-07-02", 8, "GET", "/index.html", "", 200, 1024)),
    (b'host-c.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /some file.html HTTP/1.0" 200 512\n',
     ("1995-07-02", 8, "GET", "/some file.html", "HTTP/1.0", 200, 512)),
    (b'host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "" 400 0\n',
     ("1995-07-02", 8, "", "", "", 400, 0)),
]
MALFORMED = [
    b'host-d.example.com - - [02/Jul/1995:08:15:00 -0400] "GET /index.html HTTP/1.0" 200\n',
    b'host-e.example.com - - [02/Jul/1995 08:15:00 -0400] "GET / HTTP/1.0" 200 100\n',
    b'\n',
]
FIELDS = ("log_date", "log_hour", "http_method", "resource_path", "protocol_version",
          "status_code", "bytes_transferred")


@pytest.mark.parametrize("line, expected", PARSED)
def test_parsed_examples(line, expected):
    record = parse_line(line)
    assert record is not None
    assert tuple(record[f] for f in FIELDS) == expected


@pytest.mark.parametrize("line", MALFORMED)
def test_malformed_examples(line):
    assert parse_line(line) is None


def test_carriage_return_is_removed():
    record = parse_line(
        b'h - - [04/Aug/1995:10:00:00 -0400] "GET /index.html HTTP/1.0" 404 0\r\n')
    assert record["bytes_transferred"] == 0 and record["log_date"] == "1995-08-04"
