"""One-time profile of the BLS `pr` folder and the population API. Needs no Spark or Java.

Usage: BLS_CONTACT_EMAIL=you@example.com python scripts/profile_sources.py .sample
"""

import collections
import hashlib
import json
import os
import pathlib
import re
import ssl
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import certifi

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POP_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
TLS = ssl.create_default_context(cafile=certifi.where())
QUARTERS = {"Q01", "Q02", "Q03", "Q04"}


def fetch(url, user_agent, attempts=4):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=60, context=TLS) as r:
                return r.read(), dict(r.headers)
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        time.sleep(2**attempt)


def download(out, user_agent):
    listing, _ = fetch(BLS_URL, user_agent)
    (out / "_listing.html").write_bytes(listing)
    hrefs = re.findall(r'href="([^"]+)"', listing.decode("utf-8", "replace"), re.I)
    names = sorted(
        {
            urllib.parse.urljoin(BLS_URL, h).rsplit("/", 1)[1]
            for h in hrefs
            if urllib.parse.urljoin(BLS_URL, h).startswith(BLS_URL)
        }
        - {""}
    )
    print(f"\n## Download\nfiles discovered: {len(names)}")
    for name in names:
        body, headers = fetch(BLS_URL + name, user_agent)
        (out / name).write_bytes(body)
        print(
            f"  {name:22} {len(body):>9} bytes  sha256={hashlib.sha256(body).hexdigest()[:12]}"
            f"  last-modified={headers.get('Last-Modified')}"
        )
        time.sleep(1)
    body, _ = fetch(POP_URL, "rearc-data-quest/1.0")
    (out / "_population.json").write_bytes(body)
    return names


def read_tsv(path):
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
        encoding = "latin-1"
    lines = text.split("\n")
    crlf = sum(line.endswith("\r") for line in lines)
    lines = [line.rstrip("\r") for line in lines if line.rstrip("\r") != ""]
    rows = [line.split("\t") for line in lines]
    return rows[0], rows[1:], encoding, crlf


def profile_table(name, header, rows, encoding, crlf):
    print(f"\n### {name}\nrows={len(rows)} encoding={encoding} crlf_lines={crlf}")
    print("field counts:", dict(collections.Counter(len(r) for r in rows)), "header fields:", len(header))
    for i, col in enumerate(header):
        vals = [r[i] if i < len(r) else "" for r in rows]
        stripped = [v.strip() for v in vals]
        print(
            f"  {col!r:22} empty={sum(not v for v in stripped):>6} padded={sum(a != b for a, b in zip(vals, stripped)):>6}"
            f" distinct={len(set(stripped)):>6} maxlen={max(map(len, stripped), default=0):>4}"
            f" sample={stripped[:2]}"
        )


def clean(header, rows):
    cols = [c.strip() for c in header]
    return [{c: (r[i].strip() if i < len(r) else "") for i, c in enumerate(cols)} for r in rows]


def profile_data(name, rows):
    keys = collections.Counter((r["series_id"], r["year"], r["period"]) for r in rows)
    dups = sum(1 for n in keys.values() if n > 1)
    bad = [r["value"] for r in rows if not re.fullmatch(r"-?\d+(\.\d+)?", r["value"])]
    years = [int(r["year"]) for r in rows]
    print(
        f"\n### {name} checks\nseries={len({r['series_id'] for r in rows})} years={min(years)}-{max(years)} duplicate_keys={dups}"
    )
    print("periods:", dict(sorted(collections.Counter(r["period"] for r in rows).items())))
    print("non-numeric values:", len(bad), collections.Counter(bad).most_common(5))
    print("footnote codes:", dict(collections.Counter(r["footnote_codes"] for r in rows).most_common(8)))


def main():
    contact = os.environ["BLS_CONTACT_EMAIL"]
    out = pathlib.Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    names = download(out, f"rearc-data-quest/1.0 (contact: {contact})")

    print("\n## Tables")
    tables = {}
    for name in names:
        if name in ("pr.txt",):
            continue
        header, rows, encoding, crlf = read_tsv(out / name)
        if name == "pr.contacts":
            print(f"\n### {name}: free text, {len(rows) + 1} lines, not tabular")
            continue
        profile_table(name, header, rows, encoding, crlf)
        tables[name] = clean(header, rows)

    print("\n## Lookup contents")
    for name in ("pr.class", "pr.duration", "pr.footnote", "pr.measure", "pr.period", "pr.seasonal", "pr.sector"):
        print(f"\n### {name}")
        for r in tables.get(name, []):
            print("  ", list(r.values())[:3])

    print("\n## Data files")
    current, alldata = tables["pr.data.0.Current"], tables["pr.data.1.AllData"]
    profile_data("Current", current)
    profile_data("AllData", alldata)
    kv = lambda rows: {(r["series_id"], r["year"], r["period"]): r["value"] for r in rows}
    cur, full = kv(current), kv(alldata)
    print("\n### Overlap")
    print(
        f"current keys={len(cur)} alldata keys={len(full)} current_not_in_alldata={len(cur.keys() - full.keys())}"
        f" shared_with_different_value={sum(cur[k] != full[k] for k in cur.keys() & full.keys())}"
    )

    print("\n## Series integrity")
    series = tables["pr.series"]
    print("columns:", list(series[0]))
    print("sample:", series[0])
    ids = {r["series_id"] for r in series}
    data_ids = {r["series_id"] for r in alldata}
    print(
        f"series rows={len(series)} distinct={len(ids)} no_data={len(ids - data_ids)} data_without_series={len(data_ids - ids)}"
    )
    print("series_id lengths:", dict(collections.Counter(len(i) for i in ids)))
    for code, lookup in (
        ("sector_code", "pr.sector"),
        ("class_code", "pr.class"),
        ("measure_code", "pr.measure"),
        ("duration_code", "pr.duration"),
        ("seasonal", "pr.seasonal"),
    ):
        known = {list(r.values())[0] for r in tables[lookup]}
        missing = {r[code] for r in series} - known
        print(f"  {code}: distinct={len({r[code] for r in series})} missing_from_{lookup}={sorted(missing)}")

    print("\n## Best-year implications")
    by = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in alldata:
        if r["period"] in QUARTERS and re.fullmatch(r"-?\d+(\.\d+)?", r["value"]):
            by[r["series_id"]][r["year"]].append((r["period"], float(r["value"])))
    print(f"series with quarterly data={len(by)}; series with no quarterly data={len(data_ids - by.keys())}")
    print("examples with no quarterly data:", sorted(data_ids - by.keys())[:5])
    quarter_counts = collections.Counter(len(v) for s in by.values() for v in s.values())
    print("quarters per series-year:", dict(sorted(quarter_counts.items())))
    ties = partial_best = changed = 0
    for sid, yrs in by.items():
        totals = {y: round(sum(v for _, v in q), 6) for y, q in yrs.items()}
        top = max(totals.values())
        winners = [y for y, t in totals.items() if t == top]
        ties += len(winners) > 1
        partial_best += any(len(yrs[y]) < 4 for y in winners)
        full_years = {y: t for y, t in totals.items() if len(yrs[y]) == 4}
        changed += bool(full_years) and max(full_years.values()) != top
    print(
        f"series tied for best year={ties}; best year has <4 quarters={partial_best}; best year differs if only full years count={changed}"
    )
    with_q05 = collections.defaultdict(float)
    for r in alldata:
        if r["period"] in QUARTERS | {"Q05"} and re.fullmatch(r"-?\d+(\.\d+)?", r["value"]):
            with_q05[(r["series_id"], r["year"])] += float(r["value"])
    differs = 0
    for sid in by:
        q = {y: sum(v for _, v in p) for y, p in by[sid].items()}
        a = {y: t for (s, y), t in with_q05.items() if s == sid}
        differs += max(q, key=lambda y: (q[y], y)) != max(a, key=lambda y: (a[y], y))
    print(f"series whose best year changes if Q05 is wrongly included={differs}")

    pop = json.loads((out / "_population.json").read_text())
    population = {int(r["Year"]): int(r["Population"]) for r in pop["data"]}
    window = [population[y] for y in range(2013, 2019)]
    print("\n## Population")
    print(f"years={sorted(population)}; nations={ {r['Nation'] for r in pop['data']} }")
    print(
        f"2013-2018 mean={statistics.mean(window):.2f} sample_stddev={statistics.stdev(window):.2f} population_stddev={statistics.pstdev(window):.2f}"
    )

    print("\n## PRS30006032 Q01")
    target = sorted(
        (int(r["year"]), r["value"]) for r in alldata if r["series_id"] == "PRS30006032" and r["period"] == "Q01"
    )
    print(
        f"rows={len(target)} years={target[0][0]}-{target[-1][0]}; years with population={sum(y in population for y, _ in target)}"
    )
    print("latest rows:", [(y, v, population.get(y)) for y, v in target[-6:]])


if __name__ == "__main__":
    main()
