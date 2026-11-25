#!/usr/bin/env python3
"""Per-function complexity report and gate, from rust-code-analysis metrics.

    devbox run -- rust-code-analysis-cli -m -O json -p src > metrics.jsonl
    scripts/complexity.py metrics.jsonl [MAX_CYCLOMATIC [MAX_COGNITIVE]]

Lists every function above 10 on either measure, and exits non-zero if any
exceeds the limits (default: cyclomatic 20, cognitive 15). Cognitive
complexity is the main gate: unlike cyclomatic complexity it does not
penalise flat `match` tables such as command lookups.
"""
import json
import sys

REPORT_ABOVE = 10


def walk(space, path, out):
    for child in space.get("spaces", []):
        if child["kind"] == "function":
            m = child["metrics"]
            out.append((m["cyclomatic"]["sum"], m["cognitive"]["sum"], m["loc"]["sloc"],
                        f'{path}:{child["start_line"]}', child["name"]))
        walk(child, path, out)


def main():
    out = []
    for line in open(sys.argv[1]):
        if line.strip():
            doc = json.loads(line)
            walk(doc, doc["name"], out)
    max_cyclo = float(sys.argv[2]) if len(sys.argv) > 2 else 20
    max_cogn = float(sys.argv[3]) if len(sys.argv) > 3 else 15

    out.sort(reverse=True)
    print(f'{"cyclo":>5} {"cogn":>5} {"sloc":>5}  location / function')
    failing = []
    for cc, cog, sloc, loc, name in out:
        if cc > REPORT_ABOVE or cog > REPORT_ABOVE:
            over = cc > max_cyclo or cog > max_cogn
            if over:
                failing.append(name)
            print(f'{cc:5.0f} {cog:5.0f} {sloc:5.0f}  {loc}  {name}{"  <-- over limit" if over else ""}')
    print(f"\n{len(out)} functions; limits cyclomatic {max_cyclo:g}, cognitive {max_cogn:g}: "
          f"{len(failing)} over")
    sys.exit(1 if failing else 0)


if __name__ == "__main__":
    main()
