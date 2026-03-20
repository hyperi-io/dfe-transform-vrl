# [1.0.0-dev.2](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.0-dev.1...v1.0.0-dev.2) (2026-03-20)


### Bug Fixes

* migrate to DFE metrics standard with rustlib 1.18.0 ([3f0c696](https://github.com/hyperi-io/dfe-transform-vrl/commit/3f0c696ec841b3fa02d5e92da184342174202adb))

# 1.0.0-dev.1 (2026-03-20)


### Bug Fixes

* add dual-mode test infrastructure (remote/docker) [skip ci] ([fd5324f](https://github.com/hyperi-io/dfe-transform-vrl/commit/fd5324f37bb61ca5152dfa1605a7f1e22d1ee729))
* add Helm chart, KEDA, and integration tests [skip ci] ([703b119](https://github.com/hyperi-io/dfe-transform-vrl/commit/703b119137ed68235907577c51204da78fd578d2))
* add hot-reload via rustlib SharedConfig<HotConfig> [skip ci] ([8830c08](https://github.com/hyperi-io/dfe-transform-vrl/commit/8830c0871513227975866c1ffe23ea12ce284997))
* add KEDA contract, kafka and deployment unit tests [skip ci] ([95a0043](https://github.com/hyperi-io/dfe-transform-vrl/commit/95a00433d86151a58478f3be1e8e1f4668a073b5))
* add MemoryGuard backpressure and DfeSource topic naming [skip ci] ([aa5f1b1](https://github.com/hyperi-io/dfe-transform-vrl/commit/aa5f1b1ab1639847fd397af431cc816dbb145226))
* add VRL edge case tests, Transport trait, enrichment TODO [skip ci] ([7937bf3](https://github.com/hyperi-io/dfe-transform-vrl/commit/7937bf37548c6b529f3f52cbbf9feb4cc6c3b357))
* bump dependency floors to latest versions [skip ci] ([75ca93a](https://github.com/hyperi-io/dfe-transform-vrl/commit/75ca93aa6d91dbb6582cb7567ebfb85a6a30c930))
* bump hyperi-rustlib to >=1.16.7 ([42d9ffa](https://github.com/hyperi-io/dfe-transform-vrl/commit/42d9ffa1ae3d5327d2e63faa2833ef85e3876c7a))
* bump rustlib 1.16.6, wire DfeMetrics pipeline_ready/scaling/readiness [skip ci] ([a3e9616](https://github.com/hyperi-io/dfe-transform-vrl/commit/a3e9616169130744d279f916cd5a4e7e2b6ad8cc))
* clippy clean, add profiling profile, fix deny.toml [skip ci] ([9ea26f9](https://github.com/hyperi-io/dfe-transform-vrl/commit/9ea26f9a7b83c8c23de03817104a7dd6e8091bbe))
* implement MVP — VRL engine, pipeline, health, metrics [skip ci] ([8abdad8](https://github.com/hyperi-io/dfe-transform-vrl/commit/8abdad8025711b68aeae25a8f8a3c09a9c28574f))
* implement VRL engine, kafka layer, pipeline, and observability [skip ci] ([400556f](https://github.com/hyperi-io/dfe-transform-vrl/commit/400556ff9db5474ab663a47ae285fd79db67fe9e))
* migrate to hyperi-ci, remove legacy ci submodule [skip ci] ([a59ce03](https://github.com/hyperi-io/dfe-transform-vrl/commit/a59ce031dd092150e43c5c35dfed71738ccc5a8b))
* pin rustlib crates.io-only rule in STATE.md, deploy claude rules [skip ci] ([fd86108](https://github.com/hyperi-io/dfe-transform-vrl/commit/fd861086eeda2c96cda5006b87f447d040a4da28))
* remove MSRV cap, build against latest stable [skip ci] ([aeba197](https://github.com/hyperi-io/dfe-transform-vrl/commit/aeba1978b8c8b4a8670ea94959ac8f1795b1308f))
* remove skip-ci blanket, CI is live via hyperi-ci ([4b39b44](https://github.com/hyperi-io/dfe-transform-vrl/commit/4b39b446ef962266299a99121193a718181517eb))
* review fixes — SIGTERM, batch timeout, abort metrics, nested keys [skip ci] ([b55fcdd](https://github.com/hyperi-io/dfe-transform-vrl/commit/b55fcdd9a78bc1f5e0c8456fb59d82a0018ab9fd))
* rustlib 1.16.3 remediation — ApplyFlatEnv, DfeMetrics, security, log spam [skip ci] ([0fad58c](https://github.com/hyperi-io/dfe-transform-vrl/commit/0fad58c1f1b744cf915a7276f38b0658cbfb449b))
* scaffold dfe-transform-vrl project [skip ci] ([ee171a5](https://github.com/hyperi-io/dfe-transform-vrl/commit/ee171a5e54c62c07185ebd60dbf38c74d26a7fec))
* set rust-version = 1.94, forward MSRV policy [skip ci] ([6626a43](https://github.com/hyperi-io/dfe-transform-vrl/commit/6626a436756cae6f3f2357c6b14f7ed1502a25ce))


### Features

* add VRL enrichment tables with CSV/JSON loading and custom functions [skip ci] ([956001e](https://github.com/hyperi-io/dfe-transform-vrl/commit/956001e95b7e19bf8fd2d4fedfd27ecab0980a1e))
