# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `config`, `engine`, `kafka`, `pipeline`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(engine): add custom VRL function support"
```

This produces:

```text
feat(engine): add custom VRL function support

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) converts to the
Change License on the Change Date specified in [LICENSE](LICENSE).

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Building

### Local Build

```bash
cargo build --release
```

### Running Tests

```bash
cargo nextest run
```

## Questions

If you have questions about contributing, please open an issue or contact us.

## Context

### What this is

A single Rust binary that runs VRL transforms over records in the DFE data path
-- Kafka in and out on the `bus` transport, gRPC in and out on the `direct`
transport. Two boundaries get assumed wrongly. First, it is VRL only: a pipeline
needing `lua`, `aggregate`, `dedupe`, `throttle` or `sample` belongs in
dfe-transform-vector, which keeps the Vector subprocess and pays for it. Second,
this crate does not own its own event loop. scalo's `BatchEngine::pipeline`
drives `recv -> process -> send -> release`, and this crate supplies the `process`
closure, the produce sink, the config, the VRL compiler and the enrichment
registry. Reading `src/pipeline.rs` expecting to find the loop is the usual wrong
turn.

### Where things live

| Path | Holds |
|---|---|
| `src/pipeline.rs` | The `process` closure and sink handed to scalo's batch engine |
| `src/engine/` | `compiler.rs` builds one program from the transform files, `runner.rs` runs it per event, `budget.rs` holds the compile memory floor |
| `src/config/` | `Config`, validation, `HotConfig`, `INERT_SETTINGS`, `SCALO_CASCADE_SECTIONS` |
| `src/enrichment/` | Table loading (CSV, JSON, YAML, MMDB, STIX, SQLite), refresh, the custom VRL functions |
| `src/deployment.rs` | `contract()` -- the single source the Dockerfile and the released chart are generated from |
| `Dockerfile` | Generator output, not hand-authored |
| `pipelines/filebeat/` | Opt-in data bundle (212,218 bytes of VRL plus a lookup table), not engine capability |
| `tests/` | `integration/` runs mostly without Kafka, `e2e/` needs a broker, `TESTING.md` explains the modes |
| `docs/architecture.md` | Why the service is built this way, and the invariants |

### Commands that prove a change

```bash
make check                                        # hyperi-ci check -- quality + test, the pre-push gate
cargo nextest run --features enrichment-mmdb,enrichment-sqlite  # what CI actually runs
cargo nextest run --all-features --run-ignored    # adds the ignored Kafka e2e tests
cargo run -- config-check --config config.yaml    # validate a config without starting
cargo run --bin dfe-transform-vrl -- emit-dockerfile > Dockerfile    # regenerate the Dockerfile
```

Green lies here in three ways, and all three are on by default.

`default = []` in `Cargo.toml`, so a bare `cargo nextest run` does not compile the MMDB or SQLite enrichment tests in at all. `.hyperi-ci.yaml` adds `enrichment-mmdb` and `enrichment-sqlite` to both the test and the build feature sets -- the build too, because the Dockerfile only copies the binary, so a default build ships a container that rejects `type: mmdb` and `type: sqlite` tables at startup.

Kafka-dependent tests call `skip_if_no_kafka!()` and skip cleanly when no broker
is reachable. They report as not-failed, which is not the same as proven.

The Kafka e2e tests are `#[ignore]` and need `--run-ignored` plus a broker, and the other e2e tests run by default and start their own containers. Set
`TEST_MODE=docker` for the dfe-docker infra profile on `localhost:19092`, or
`TEST_MODE=remote` for the cluster endpoints.

One more: the push trigger in `.github/workflows/ci.yml` carries
`paths-ignore: docs/**, **.md`, so a docs-only push runs no jobs. The
`pull_request` trigger has no such filter, so the PR is where a docs change gets
checked.

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Hand-edit `Dockerfile`, or commit a chart | Fix `src/deployment.rs::contract()` and regenerate | The Dockerfile is generator output and the release assembles the chart from the contract, so a hand edit is reverted or never ships, and `test_every_contract_secret_env_var_reaches_the_config` holds every declared name to the field it spells |
| Bump scalo and leave `release.helm.library` behind | Move `release.helm.library` in `.hyperi-ci.yaml` to the same scalo version | A scalo-service release renders only the contract version its scalo release writes |
| Turn the Kafka lag trigger back on in `contract()` | Leave `KafkaLagTrigger::disabled()` and scale on CPU plus scaling pressure | Lag rises when a downstream stage breaks, so a lag trigger adds pods that wait on the same broken stage. The lag trigger was also the one that kept reading a kafka block this app's values do not have |
| Put a scalo section (`metrics`, `logger`, `scaling`, `worker_pool`, `batch_processing`, `self_regulation`, `version_check`, `otel_tracing`) in a `config.yaml` the binary finds in its working directory | Pass the file with `--config`, or set the section through the env layer on a **double** underscore | scalo reads the `--config` file as its settings layer but finds other files only by fixed base name (`settings.yaml`, `defaults.yaml`), so a working-directory `config.yaml` reaches the wrapper alone. Full list under [Environment variable overrides](docs/internals.md#environment-variable-overrides) |
| Trust `pipeline.batch_size`, `pipeline.batch_timeout_ms`, `sink.key_field` or `source.commit_interval_ms` | Size a chunk with `batch_processing.max_chunk_size` | `config::INERT_SETTINGS` -- accepted, validated, reaching nothing. Table under [Reload](docs/internals.md#reload) |
| Call a bare `cargo nextest run` green | Pass `--features enrichment-mmdb,enrichment-sqlite` | `default = []`, so the MMDB and SQLite tests are not compiled in and the run is green without having tested them |
| Set `sasl.enabled` with an empty username or password | Supply both, or neither | librdkafka's SCRAM check is a NULL check that an empty string passes, so that pod authenticated against nothing and still reported Ready. Refused at startup now |
| Add a serialise path that redacts by field name | Keep the password a `scalo::SensitiveString` | Redaction is by type on every path -- `Debug`, the `/config` dump, the emitted schema. The figment round-trip has to be wrapped in `expose_during` or a file-sourced password reaches the broker as the literal `***REDACTED***` |
| Size the container from steady state when the program is large | Leave headroom for the compile | Compilation scales with the VRL, and the bundled filebeat program is OOM-killed under a 32 MiB limit. Below the floor the kernel kills the process mid-compile, seen as an exit-137 restart loop with nothing in the log. `engine::budget` refuses first and names all three numbers |
| Pick the optimisation tier with `publish-target` | Use `build.skip_optimize` in `.hyperi-ci.yaml`, or the `skip-optimize` dispatch input for one run | hyperi-ci ignores `publish-target`. A build that ships gets PGO and BOLT from `scripts/pgo-workload.sh` unless optimisation is skipped, and a stable release that skips it is refused unless the dispatch passes `release-unoptimized: true` |

### Where this sits

Inbound -- what this repo depends on:

- **scalo-rs** (`cargo-dep`). `Cargo.toml` declares the `scalo` crate by range, a
  second range covering the dev dependency. A scalo release arrives through that
  range: `cargo update -p scalo` and rebuild if it admits the version, widen the
  range first if not.
- **scalo-rs** (`generated-file`, lockstep). The `Dockerfile` is written by
  scalo's generator from this crate's `deployment::contract()`, at contract
  schema version 4. A generator or schema change upstream means regenerating
  with the command in the file's own header and committing the diff. The
  released chart is assembled on the scalo-service library chart at
  `release.helm.library`, which moves with the scalo version in `Cargo.toml`.
- **dfe-infra** (`apps.yaml`, deploy-time authority). The suite manifest declares
  what this app is: `multiplicity: per_config` (one deployment per source config,
  never a singleton), `scale_deployed: true`, both transports, and a source
  binding deriving `{source}_land` in, `{source}_load` out and
  `dfe-transform-vrl-{source}` as the consumer group. Adding a source is a
  manifest edit, not a change here.

Outbound -- what depends on this repo:

- **dfe-infra** (`image-pin`, lockstep). Pins this repo's ghcr image as a tag plus
  the digest that makes it immutable. A release here means bumping the tag and
  re-resolving the digest there, with `check_versions_drift.py` confirming the
  chart's appVersion and the digest mirror agree.

dfe-transform-splack runs this repo's engine image with a rule-config chart of
its own. It is out of suite scope and no work here is driven by it.
