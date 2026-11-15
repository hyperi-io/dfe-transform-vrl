# Filebeat pipeline test cases

`cases.yaml` holds the synthetic log lines that `tests/integration/filebeat_pipeline.rs`, `tests/e2e/filebeat_kafka.rs` and the PGO workload (`src/bin/pgo_driver.rs`) replay through the bundled filebeat-compat pipeline (`pipelines/filebeat/filebeat.vrl`). The lines are written for this repo. Hosts, addresses and names come from the documentation ranges (`192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24`, `2001:db8::/32`, `example.*`). Earlier revisions replayed third-party sample data, which is no longer part of the repo.

Each case has:

- `name`: unique, and carried through the broker in the e2e suite to pair an output with its case.
- `module`: `meraki`, `ios` or `umbrella`.
- `conf`: optional `_conf` object the DFE 2.1 deployment injected (`tz_offset`).
- `line`: the raw log line, which becomes `message`.
- `expect`: the fields the output must carry. Every leaf named must be present and equal. Fields not named are not checked, and the root `tags` array compares as a set.

## Adding a case

Write a line for the module, run it through the pipeline, read the output, and put the fields you have checked by hand into `expect`. A Cisco IOS line with no year in its timestamp gets the current year from the parser, so leave `@timestamp` out of `expect` for those.
