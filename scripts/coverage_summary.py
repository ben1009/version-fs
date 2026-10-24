#!/usr/bin/env python3
"""Print a concise Markdown summary from an LCOV report."""

from pathlib import Path
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <lcov-file>", file=sys.stderr)
        return 2

    report_path = Path(sys.argv[1])
    totals: list[tuple[int, int]] = []
    record: dict[str, int] = {}

    for line_number, line in enumerate(report_path.read_text(encoding="utf-8").splitlines(), 1):
        if line == "end_of_record":
            if "LF" in record or "LH" in record:
                if "LF" not in record or "LH" not in record:
                    print(f"incomplete line totals at LCOV line {line_number}", file=sys.stderr)
                    return 1
                found = record["LF"]
                hit = record["LH"]
                if hit > found:
                    print(f"invalid line totals at LCOV line {line_number}", file=sys.stderr)
                    return 1
                totals.append((hit, found))
            record = {}
            continue

        key, separator, value = line.partition(":")
        if separator and key in {"LF", "LH"}:
            try:
                record[key] = int(value)
            except ValueError:
                print(f"invalid {key} value at LCOV line {line_number}", file=sys.stderr)
                return 1

    if record:
        print("LCOV report ended before end_of_record", file=sys.stderr)
        return 1
    if not totals:
        print("LCOV report contains no line coverage totals", file=sys.stderr)
        return 1

    hit = sum(item[0] for item in totals)
    found = sum(item[1] for item in totals)
    if found == 0:
        print("LCOV report contains no executable lines", file=sys.stderr)
        return 1

    print("# Coverage")
    print()
    print("| Metric | Result |")
    print("| --- | ---: |")
    print(f"| Source files | {len(totals)} |")
    print(f"| Lines hit | {hit:,} / {found:,} |")
    print(f"| Line coverage | **{100 * hit / found:.2f}%** |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
