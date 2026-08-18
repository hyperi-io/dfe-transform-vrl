"""Fetch the cisco filebeat pipeline test corpora from elastic/integrations
at a pinned SHA using the gh CLI (authenticated, read-only contents API),
for refreshing tests/fixtures/filebeat/filebeat-testdata.tar.gz.

Usage:
  python3 scripts/filebeat/fetch_testdata.py <output-dir>
  tar -czf tests/fixtures/filebeat/filebeat-testdata.tar.gz \
      -C <output-dir> cisco_umbrella cisco_ios cisco_meraki LICENSE.txt

Keep SHA aligned with the dfe-transform-elastic testdata pin unless the
bundled VRL is regenerated against a newer template vintage (see
tests/fixtures/filebeat/README.md).
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path

REPO = "elastic/integrations"
SHA = "c7bc53023288c7d34afd631c0c3e3552f6219419"

DIRS = {
    "cisco_umbrella/log": "packages/cisco_umbrella/data_stream/log/_dev/test/pipeline",
    "cisco_ios/log": "packages/cisco_ios/data_stream/log/_dev/test/pipeline",
    "cisco_meraki/log": "packages/cisco_meraki/data_stream/log/_dev/test/pipeline",
}

EXTRAS = {
    "LICENSE.txt": "LICENSE.txt",
    "licenses/Elastic-2.0.txt": "licenses/Elastic-2.0.txt",
    "cisco_umbrella/manifest.yml": "packages/cisco_umbrella/manifest.yml",
    "cisco_ios/manifest.yml": "packages/cisco_ios/manifest.yml",
    "cisco_meraki/manifest.yml": "packages/cisco_meraki/manifest.yml",
}


def gh_api(path: str) -> bytes:
    res = subprocess.run(
        ["gh", "api", path],
        capture_output=True,
        check=False,
    )
    if res.returncode != 0:
        print(f"FAIL: gh api {path}: {res.stderr.decode('utf-8', 'replace')}",
              file=sys.stderr)
        sys.exit(1)
    return res.stdout


def fetch_file(repo_path: str, dest: Path) -> int:
    obj = json.loads(gh_api(f"repos/{REPO}/contents/{repo_path}?ref={SHA}"))
    if obj.get("encoding") != "base64":
        print(f"FAIL: unexpected encoding for {repo_path}: "
              f"{obj.get('encoding')}", file=sys.stderr)
        sys.exit(1)
    data = base64.b64decode(obj["content"])
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return len(data)


def main() -> None:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    out = Path(sys.argv[1])
    total = 0
    n = 0
    for local_dir, repo_dir in DIRS.items():
        listing = json.loads(gh_api(f"repos/{REPO}/contents/{repo_dir}?ref={SHA}"))
        for entry in listing:
            if entry["type"] != "file":
                continue
            size = fetch_file(
                f"{repo_dir}/{entry['name']}", out / local_dir / entry["name"]
            )
            total += size
            n += 1
            print(f"  {local_dir}/{entry['name']}  {size}B")
    for local_path, repo_path in EXTRAS.items():
        size = fetch_file(repo_path, out / local_path)
        total += size
        n += 1
        print(f"  {local_path}  {size}B")
    print(f"fetched {n} files, {total} bytes, pinned {SHA}")


if __name__ == "__main__":
    main()
