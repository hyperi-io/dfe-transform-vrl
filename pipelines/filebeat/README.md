# Bundled pipeline: filebeat-compat (INTERIM)

A pre-canned, opt-in VRL pipeline that keeps DFE 2.1 filebeat feeds working
on dfe-transform-vrl. It is pure VRL plus one CSV enrichment table - no
binary capability, no config surface. Use it if you want it, ignore it if
you do not. It also doubles as a real-data integration test workload for
the engine (`tests/integration/filebeat_pipeline.rs`).

> **INTERIM.** Elastic compatibility is being replaced by
> `dfe-transform-elastic` (Rust-native, currently in beta). New
> integrations should NOT build on this pipeline; existing DFE 2.1
> filebeat feeds should plan to migrate when dfe-transform-elastic ships.

## What it covers

Single-file port of the DFE 2.1 Vector templates (dfe-vector-templates
`src/core_templates`, the `14*-transform-filebeat-*` family):

| DFE 2.1 template | Module |
|---|---|
| 140-transform-filebeat-cisco-meraki-logs | Cisco Meraki syslog (flows, events, airmarshal, urls, security, ip-flows) |
| 141-transform-filebeat-cisco-ios-logs | Cisco IOS syslog |
| 143..149-transform-filebeat-cisco-umbrella-*-logs | Cisco Umbrella exports (audit, cloud firewall, dlp, dns, intrusion, ip, proxy) |

The seven umbrella templates were one body split across files (Vector
large-VRL workarounds) differing only in a hardcoded `.log.file.path` seed;
they collapse here into one body behind log-type detection.

## Opting in

```yaml
# config.yaml
transforms:
  dir: "/path/to/pipelines/filebeat"   # loads filebeat.vrl

enrichment_tables:
  - name: "timezones"                  # required by the ios/meraki
    path: "/path/to/pipelines/filebeat/timezones.csv"
    key_columns: ["abbreviation"]
```

Both files are in this directory; mount or copy them wherever the pod can
read them. Nothing else is needed - the engine treats this like any other
`transforms.dir`.

Events are the DFE 2.1 Kafka shape: `{message, tags, timestamp}`. Two
optional event fields drive ios/meraki timezone handling: `._conf.tz_offset`
(an offset like `-0600` or an abbreviation from `timezones.csv`) and
`._conf.tz_map` (a list of `{tz_short, tz_long}` mappings from log
abbreviations to IANA zone names). Without them, meraki still derives
`@timestamp` from the log line's epoch, and ios interprets zone-less
timestamps as UTC - the knobs override the zone interpretation, they do
not gate timestamping.

## How routing works

DFE 2.1 fanned every event to every module transform and relied on
`drop_on_abort` to discard the wrong parses. Embedded VRL runs one program
per event, so this port detects the module first:

1. `<134>1 <epoch>` prefix -> Meraki
2. any other `<PRI>` syslog prefix -> Cisco IOS
3. `%FACILITY-SEVERITY-MNEMONIC` marker (on a non-CSV-shaped line) ->
   Cisco IOS
4. otherwise CSV shape -> Umbrella; log type from `.log.file.path` when the
   upstream provides one, else column-shape detection: 7 cols = ip,
   9 with the timestamp in column 1 = audit, 13 (or 10-12 with the
   `<n> (TYPE)` query-type marker in column 6) = dns, 14/16 = cloud
   firewall, 17 = intrusion when columns 3-4 are numeric gid/sid else
   dlp, >=20 = proxy; every rule also requires a leading timestamp column
5. anything else: tagged `filebeat_unmatched` and passed through unchanged
   (the 2.1 fan-out silently dropped these)

Detection picks the branch; parsing failures INSIDE the chosen branch
still error the event out (dropped and counted), matching the 2.1
`drop_on_abort` behaviour. Only shape-unmatched events pass through.

## Deliberate departures from DFE 2.1

- **No geoip enrichment.** dfe-loader owns geoip in the current pipeline
  layout; the `geoip_city`/`geoip_asn` lookups are stripped.
- **Unmatched events pass through tagged**, never silently dropped.
- **`.log.file.path` is rewritten** to `/filebeat/cisco_umbrella/<type>`
  in the umbrella branch (2.1 also overwrote it, with `/test/path/<type>`).
- **IANA zone names resolve correctly in the ios branch.** The 2.1
  templates pushed a `tz_map`-mapped name like `Australia/Sydney` into the
  abbreviation CSV lookup, which always failed and dropped the event; the
  port routes slash-bearing zone names through `parse_timestamp`'s
  `timezone:` argument (DST-aware via the tz database).

## Known limitations (verified against the elastic golden corpus)

- `user_agent` classification follows VRL's parser, which differs
  cosmetically from elastic's ("Mac OSX" vs "Mac OS X"); a
  whitespace-only UA string is treated as absent.
- `url.query` is rebuilt with alphabetised parameters (VRL object maps are
  ordered); elastic preserves raw order. Same parameters, different order.
- `dns.question.registered_domain` uses the full public-suffix list
  including the private section; elastic uses the ICANN section only.
- Ambiguous timezone abbreviations (the CSV carries duplicates such as
  ADT, AMT, AST, CST, IST) never resolve - the 2.1 "ambiguous timezone
  abbreviation" path is preserved.

## Provenance and regeneration

Generated from dfe-vector-templates `origin/main` @ `96eb306` by
`scripts/filebeat/extract.py` (verifies the seven umbrella templates are
identical before collapsing them, strips geoip, unescapes Vector `$$`,
prepends `scripts/filebeat/prologue.vrl`). Validated line-by-line against
the elastic/integrations pipeline test corpus @ `c7bc530` - see
`tests/fixtures/filebeat/README.md`.

Treat `filebeat.vrl` as generated-then-patched: fixes belong here (it is
the shipping artefact), but substantial rework should go back through the
template extraction so the umbrella dedupe stays verifiable.
