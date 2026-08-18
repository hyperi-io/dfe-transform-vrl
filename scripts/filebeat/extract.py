"""Regenerate pipelines/filebeat/filebeat.vrl from the DFE 2.1 Vector
templates (dfe-vector-templates repo).

Stages:
  A. read each template's `source: |` VRL block via `git show` and
     unescape Vector's `$$` -> `$`
  B. verify the 7 umbrella files are identical after token normalisation
  C. strip geoip enrichment blocks (dfe-loader owns geoip) and any
     if-blocks emptied by the strip
  D. strip per-file `_ingest` preambles + the umbrella .log.file.path seed
  E. print the umbrella per-type csv column analysis (heuristic input)
  F. assemble prologue.vrl + bodies -> pipelines/filebeat/filebeat.vrl

Usage:
  python3 scripts/filebeat/extract.py

Environment:
  DFE_VECTOR_TEMPLATES  path to a dfe-vector-templates clone
                        (default /projects/dfe-vector-templates)
  TEMPLATES_REF         git ref to read templates from (default origin/main)
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
REPO = HERE.parent.parent
OUT_DIR = REPO / "pipelines" / "filebeat"

TEMPLATES_REPO = Path(os.environ.get("DFE_VECTOR_TEMPLATES", "/projects/dfe-vector-templates"))
TEMPLATES_REF = os.environ.get("TEMPLATES_REF", "origin/main")
TEMPLATES_DIR = "src/core_templates"

UMBRELLA_TYPES = [
    "auditlogs",
    "cloudfirewalllogs",
    "dlplogs",
    "dnslogs",
    "intrusionlogs",
    "iplogs",
    "proxylogs",
]

TEMPLATE_FILES = {
    "meraki": "140-transform-filebeat-cisco-meraki-logs.yml",
    "ios": "141-transform-filebeat-cisco-ios-logs.yml",
    "auditlogs": "143-transform-filebeat-cisco-umbrella-audit-logs.yml",
    "cloudfirewalllogs": "144-transform-filebeat-cisco-umbrella-cloud-firewall-logs.yml",
    "dlplogs": "145-transform-filebeat-cisco-umbrella-dlp-logs.yml",
    "dnslogs": "146-transform-filebeat-cisco-umbrella-dns-logs.yml",
    "intrusionlogs": "147-transform-filebeat-cisco-umbrella-intrusion-logs.yml",
    "iplogs": "148-transform-filebeat-cisco-umbrella-ip-logs.yml",
    "proxylogs": "149-transform-filebeat-cisco-umbrella-proxy-logs.yml",
}


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def template_text(name: str) -> str:
    """Read a template from the templates repo without touching its
    working tree (the checkout may be on an unrelated WIP branch)."""
    res = subprocess.run(
        ["git", "-C", str(TEMPLATES_REPO), "show",
         f"{TEMPLATES_REF}:{TEMPLATES_DIR}/{name}"],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if res.returncode != 0:
        fail(f"git show {name}: {res.stderr.strip()}")
    return res.stdout


# --------------------------------------------------------------------------
# Stage A: extract + unescape
# --------------------------------------------------------------------------

def extract_vrl(name: str) -> list[str]:
    """Return the de-indented VRL lines of the (single) source: | block."""
    lines = template_text(name).splitlines()
    try:
        start = next(
            i for i, ln in enumerate(lines) if ln.rstrip() == "    source: |"
        )
    except StopIteration:
        fail(f"{name}: no 'source: |' block found")
    body: list[str] = []
    for ln in lines[start + 1 :]:
        if ln.strip() == "":
            body.append("")
            continue
        if not ln.startswith("      "):
            break  # end of the literal block
        body.append(ln[6:])
    while body and body[-1] == "":
        body.pop()
    # Vector env-var escape: $$ means a literal $
    return [ln.replace("$$", "$") for ln in body]


# --------------------------------------------------------------------------
# Stage B: umbrella equivalence
# --------------------------------------------------------------------------

def normalise_umbrella(body: list[str], log_type: str) -> list[str]:
    out = []
    for ln in body:
        ln = ln.replace(f'"cisco_umbrella_{log_type}"', '"cisco_umbrella_TYPE"')
        ln = ln.replace(f'"/test/path/{log_type}"', '"/test/path/TYPE"')
        out.append(ln)
    return out


# --------------------------------------------------------------------------
# Stage C: strip geoip blocks (+ if-blocks the strip empties)
# --------------------------------------------------------------------------

GEOIP_CALL = re.compile(r"^(\s*)geoip, err = get_enrichment_table_record\(")


def strip_geoip(body: list[str], expect: int, name: str) -> list[str]:
    out: list[str] = []
    i = 0
    stripped = 0
    while i < len(body):
        m = GEOIP_CALL.match(body[i])
        if not m:
            out.append(body[i])
            i += 1
            continue
        indent = m.group(1)
        j = i + 1
        if j >= len(body) or body[j] != f"{indent}if err == null {{":
            fail(f"{name}: geoip call at line {i + 1} not followed by "
                 f"'if err == null {{' at same indent")
        depth = 0
        k = j
        while k < len(body):
            depth += body[k].count("{") - body[k].count("}")
            if depth == 0:
                break
            k += 1
        if k - j > 40 or body[k] != f"{indent}}}":
            fail(f"{name}: geoip if-block at line {j + 1} did not close "
                 f"cleanly (ended line {k + 1})")
        i = k + 1
        stripped += 1
    if stripped != expect:
        fail(f"{name}: stripped {stripped} geoip blocks, expected {expect}")
    return out


def remove_empty_ifs(body: list[str], name: str) -> list[str]:
    """Remove `if <cond> { }` blocks emptied by geoip stripping.

    Only handles the single-line-condition form the templates use around
    geoip enrichment; repeats until stable in case of nesting.
    """
    removed = 0
    changed = True
    while changed:
        changed = False
        out: list[str] = []
        i = 0
        while i < len(body):
            ln = body[i]
            indent = ln[: len(ln) - len(ln.lstrip())]
            if (
                re.match(r"^(\s*)if .*\{$", ln)
                and ln.count("{") == 1
                and ln.count("}") == 0
                and i + 1 < len(body)
                and body[i + 1] == indent + "}"
            ):
                i += 2
                removed += 1
                changed = True
                continue
            out.append(ln)
            i += 1
        body = out
    print(f"{name}: removed {removed} emptied if-blocks")
    return body


def patch_ios_named_tz(body: list[str]) -> list[str]:
    """Route IANA zone names (from `_conf.tz_map` / `_conf.tz_offset`)
    through parse_timestamp's `timezone:` argument.

    The generated 2.1 date blocks resolve `.event.timezone` via the
    abbreviation CSV only, so a mapped name like "Australia/Sydney" never
    resolves and the event aborts. Patches the four
    `external_timezone`-driven blocks in the ios body; DST stays correct
    because the tz database, not a fixed offset, does the conversion.
    """
    out: list[str] = []
    patched = 0
    i = 0
    while i < len(body):
        ln = body[i]
        if re.match(r"^(\s*)records = if match!\(external_timezone, ", ln):
            indent = ln[: len(ln) - len(ln.lstrip())]
            # records-resolution: insert the tz-name branch before the
            # CSV-lookup else.
            offset_line = body[i + 1]
            else_line = body[i + 2]
            if (offset_line != f'{indent}    [{{ "offset": external_timezone }}]'
                    or else_line != f"{indent}}} else {{"):
                fail(f"ios tz patch: unexpected records shape at line {i + 1}")
            out.append(ln)
            out.append(offset_line)
            out.append(f'{indent}}} else if contains(to_string(external_timezone) ?? "", "/") {{')
            out.append(f'{indent}    [{{ "tz_name": external_timezone }}]')
            out.append(else_line)
            out.append(body[i + 3])  # find_enrichment_table_records line
            out.append(body[i + 4])  # closing }
            # parse: wrap the offset-based call with a tz_name branch.
            len_line = body[i + 5]
            off_line = body[i + 6]
            parse_line = body[i + 7]
            if (len_line != f"{indent}if length(records) == 1 {{"
                    or off_line != f"{indent}    offset = records[0].offset"
                    or "date, err = parse_timestamp(" not in parse_line):
                fail(f"ios tz patch: unexpected parse shape at line {i + 6}")
            naked = parse_line.replace("datetime, offset, ", "datetime, ")
            naked = naked.replace("[datetime, offset]", "[datetime]")
            naked = naked.replace(', "%:z"', "").replace('"%:z", ', "")
            if not naked.endswith("))"):
                fail(f"ios tz patch: unexpected parse line end: {naked!r}")
            naked = naked[:-1] + ", timezone: string!(records[0].tz_name))"
            naked = naked.replace(f"{indent}    date", f"{indent}        date", 1)
            out.append(len_line)
            out.append(f"{indent}    if records[0].tz_name != null {{")
            out.append(naked)
            out.append(f"{indent}    }} else {{")
            out.append(f"{indent}        {off_line.lstrip()}")
            out.append(f"{indent}        {parse_line.lstrip()}")
            out.append(f"{indent}    }}")
            patched += 1
            i += 8
            continue
        out.append(ln)
        i += 1
    if patched != 4:
        fail(f"ios tz patch: patched {patched} blocks, expected 4")
    return out


# --------------------------------------------------------------------------
# Stage D: strip per-file preamble
# --------------------------------------------------------------------------

def strip_preamble(body: list[str], pipeline_name: str, name: str,
                   drop_path_seed: str | None = None) -> list[str]:
    expected = [
        "_ingest.timestamp = now()",
        "if _ingest.pipeline == null {",
        f'    _ingest.pipeline = "{pipeline_name}"',
        f'    _ingest.on_failure_pipeline = "{pipeline_name}"',
        "}",
    ]
    if body[: len(expected)] != expected:
        fail(f"{name}: preamble mismatch, got {body[:5]!r}")
    body = body[len(expected):]
    if drop_path_seed is not None:
        seed = f'.log.file.path = "/test/path/{drop_path_seed}"'
        idx = next((i for i, ln in enumerate(body[:6]) if ln == seed), None)
        if idx is None:
            fail(f"{name}: path seed {seed!r} not found in first lines")
        body = body[:idx] + body[idx + 1:]
    return body


# --------------------------------------------------------------------------
# Stage E: umbrella branch analysis
# --------------------------------------------------------------------------

def analyse_umbrella(body: list[str]) -> None:
    print("\n== umbrella per-type branch analysis ==")
    cond_type = None
    in_action = False
    depth = 0
    info: dict[str, dict] = {
        t: {"branches": 0, "csv_parse": 0, "csv_idx": set()}
        for t in UMBRELLA_TYPES
    }
    for ln in body:
        m = re.match(r'\s*value = "([a-z]+logs)"$', ln)
        if m and m.group(1) in info and not in_action:
            cond_type = m.group(1)
        if cond_type and re.match(r"^\s*\}\)? \{$", ln):
            in_action = True
            depth = 1
            info[cond_type]["branches"] += 1
            continue
        if in_action and cond_type:
            if "parse_csv!" in ln:
                info[cond_type]["csv_parse"] += 1
            for mm in re.finditer(r"csv\[(\d+)\]", ln):
                info[cond_type]["csv_idx"].add(int(mm.group(1)))
            depth += ln.count("{") - ln.count("}")
            if depth <= 0:
                in_action = False
                cond_type = None
    for t, d in info.items():
        idx = sorted(d["csv_idx"])
        top = idx[-1] if idx else None
        print(f"  {t:20s} branches={d['branches']:3d} csv_parse="
              f"{d['csv_parse']} max_csv_idx={top} n_cols_mapped={len(idx)}")


# --------------------------------------------------------------------------
# Assembly
# --------------------------------------------------------------------------

def indent(body: list[str], pad: str) -> list[str]:
    return [pad + ln if ln else "" for ln in body]


def branch_seed(pipeline_name: str) -> list[str]:
    return [
        f'    _ingest.pipeline = "{pipeline_name}"',
        f'    _ingest.on_failure_pipeline = "{pipeline_name}"',
    ]


def main() -> None:
    meraki = extract_vrl(TEMPLATE_FILES["meraki"])
    ios = extract_vrl(TEMPLATE_FILES["ios"])
    umbrella = {t: extract_vrl(TEMPLATE_FILES[t]) for t in UMBRELLA_TYPES}
    print(f"extracted: meraki={len(meraki)} ios={len(ios)} "
          f"umbrella(audit)={len(umbrella['auditlogs'])} lines")

    ref = normalise_umbrella(umbrella["auditlogs"], "auditlogs")
    for t in UMBRELLA_TYPES[1:]:
        norm = normalise_umbrella(umbrella[t], t)
        if norm != ref:
            diffs = [
                (i, a, b) for i, (a, b) in enumerate(zip(ref, norm)) if a != b
            ]
            fail(f"umbrella {t} differs from auditlogs after normalisation: "
                 f"{diffs[:5]!r} (+len {len(ref)} vs {len(norm)})")
    print("umbrella equivalence: 7/7 identical after token normalisation")

    meraki = strip_geoip(meraki, expect=6, name="meraki")
    ios = strip_geoip(ios, expect=4, name="ios")
    umb = strip_geoip(umbrella["auditlogs"], expect=4, name="umbrella")
    meraki = remove_empty_ifs(meraki, "meraki")
    ios = remove_empty_ifs(ios, "ios")
    umb = remove_empty_ifs(umb, "umbrella")

    meraki = strip_preamble(meraki, "cisco_meraki_logs", "meraki")
    ios = strip_preamble(ios, "cisco_ios_logs", "ios")
    umb = strip_preamble(umb, "cisco_umbrella_auditlogs", "umbrella",
                         drop_path_seed="auditlogs")
    ios = patch_ios_named_tz(ios)

    analyse_umbrella(umb)

    for name, body in (("meraki", meraki), ("ios", ios), ("umbrella", umb)):
        joined = "\n".join(body)
        if "geoip" in joined:
            fail(f"{name}: geoip reference survived stripping")
        if "$$" in joined:
            fail(f"{name}: unescaped $$ survived")
        if "get_enrichment_table_record" in joined:
            fail(f"{name}: get_enrichment_table_record survived")
    if "find_enrichment_table_records" not in "\n".join(meraki):
        fail("meraki: timezones enrichment call missing")

    prologue = (HERE / "prologue.vrl").read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    out.extend(prologue)
    out.append("")
    out.append('if _module == "cisco_meraki_logs" {')
    out.extend(branch_seed("cisco_meraki_logs"))
    out.extend(indent(meraki, "    "))
    out.append('} else if _module == "cisco_ios_logs" {')
    out.extend(branch_seed("cisco_ios_logs"))
    out.extend(indent(ios, "    "))
    out.append('} else if _module == "cisco_umbrella" {')
    out.append('    _ingest.pipeline = "cisco_umbrella_" + _umbrella_type')
    out.append("    _ingest.on_failure_pipeline = _ingest.pipeline")
    out.append('    .log.file.path = "/filebeat/cisco_umbrella/" + _umbrella_type')
    out.extend(indent(umb, "    "))
    out.append("} else {")
    out.append("    # Unknown shape: tag + pass through, never silently drop.")
    out.append('    .tags = push(array(.tags) ?? [], "filebeat_unmatched")')
    out.append("}")
    out.append("")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    target = OUT_DIR / "filebeat.vrl"
    target.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"\nwrote {target} ({len(out)} lines)")


if __name__ == "__main__":
    main()
