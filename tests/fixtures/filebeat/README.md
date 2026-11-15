# Filebeat pipeline test corpus

Real filebeat/Elastic-integration pipeline test data used by
`tests/integration/filebeat_pipeline.rs` to exercise the bundled INTERIM
filebeat-compat pipeline (`pipelines/filebeat/filebeat.vrl`).

## Contents

`filebeat-testdata.tar.gz` (read in-memory at test time, never unpacked to
the working tree):

- `cisco_umbrella/log/` -- raw Cisco Umbrella export samples for all seven
  log types (audit, cloud firewall, dlp, dns, intrusion, ip, proxy) plus
  the upstream `-expected.json` golden outputs and `-config.yml` knobs.
- `cisco_ios/log/` -- Cisco IOS syslog samples (with and without syslog
  priority headers, several date formats and tz offsets) plus goldens.
- `cisco_meraki/log/` -- Cisco Meraki syslog samples (flows, events,
  airmarshal, security, urls, ip-flows) plus goldens.
- `cisco_*/manifest.yml` -- upstream package manifests (package versions,
  provenance).
- `LICENSE.txt` -- upstream root licence pointer.
- `licenses/Elastic-2.0.txt` -- the full Elastic License 2.0 text the
  pointer refers to (redistribution requires shipping the terms).

## Provenance

- Source repo: `github.com/elastic/integrations`
- Commit: `c7bc53023288c7d34afd631c0c3e3552f6219419` (pinned in
  `scripts/filebeat/fetch_testdata.py` -- the same vintage the DFE 2.1 VRL
  templates were generated against)
- Paths: `packages/cisco_{umbrella,ios,meraki}/data_stream/log/_dev/test/pipeline/`
- Licence: Elastic License 2.0 (test data redistributed unmodified for
  compatibility testing, see `LICENSE.txt` inside the archive)

## Refreshing

Re-fetch at a new pin with the gh CLI (contents API, read-only), then
re-create the archive:

```bash
python3 scripts/filebeat/fetch_testdata.py /tmp/filebeat-fetch  # edit SHA first
tar --sort=name --owner=0 --group=0 --numeric-owner \
    --mtime='2026-08-18 00:00:00 UTC' \
    -czf tests/fixtures/filebeat/filebeat-testdata.tar.gz \
    -C /tmp/filebeat-fetch \
    cisco_umbrella cisco_ios cisco_meraki LICENSE.txt licenses
```

Those tar flags make the archive byte-reproducible, so a refresh only churns
the blob when content actually changed. Re-validate the
`ACCEPTED_MISMATCHES` line indices in `tests/common/filebeat.rs` after any
refresh - they are keyed by line position within each log file.

## Golden-output caveats

The `-expected.json` files are the outputs of the ELASTIC ingest pipelines
at that pin. This repo's VRL is the DFE 2.1 port, which deliberately
differs: geoip enrichment is stripped (dfe-loader owns geoip), `related.*`
/ ECS versions may lag the newest integration revisions, and events carry
DFE 2.1 conventions (`_conf.tz_offset`, tag seeding). Tests therefore
assert on stable parsed fields, not whole-object equality.
