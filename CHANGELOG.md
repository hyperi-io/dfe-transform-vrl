## [1.0.3](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.2...v1.0.3) (2026-04-16)


### Bug Fixes

* migrate to single versioning, bump rustlib to >=2.5.4, fix pre-existing bugs ([e5562bf](https://github.com/hyperi-io/dfe-transform-vrl/commit/e5562bfa4b1174919a07dcec890820109d9cc299)), closes [#5](https://github.com/hyperi-io/dfe-transform-vrl/issues/5)

## [1.0.2](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.1...v1.0.2) (2026-04-03)


### Bug Fixes

* replace serde_json with sonic-rs for SIMD JSON deserialisation ([3dd21ca](https://github.com/hyperi-io/dfe-transform-vrl/commit/3dd21ca0826e21607f386649e66a7794d0681403))

## [1.0.1](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.0...v1.0.1) (2026-04-02)


### Bug Fixes

* add parallel VRL evaluation tests proving multi-thread execution ([bee0759](https://github.com/hyperi-io/dfe-transform-vrl/commit/bee0759454013e25373b5899ce79e44f081fb3a7))
* bump rustlib to >=2.4.3 and add debug/trace logging throughout pipeline ([f9c3892](https://github.com/hyperi-io/dfe-transform-vrl/commit/f9c3892f217cb4bcd2e9d0874019fd2b9d90c056))
* parallel deserialisation in VRL pipeline via process_batch ([c984693](https://github.com/hyperi-io/dfe-transform-vrl/commit/c984693ab8b8b4472e2769d87ac1c35a23027d8c))
* re-trigger release after tag cleanup ([7208417](https://github.com/hyperi-io/dfe-transform-vrl/commit/72084177fbeb79f6309874c0ca037275afa5e7a0))
* remove tracked target symlink — breaks CI runners ([a300d71](https://github.com/hyperi-io/dfe-transform-vrl/commit/a300d71dc3494dadfa3dabd9b3aff41123df4f23))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([4c6e4c5](https://github.com/hyperi-io/dfe-transform-vrl/commit/4c6e4c5304a7476476512d36141a28e92889fbd3))
* update to rustlib v2.x ServiceRuntime + deployment contract fields ([2e64348](https://github.com/hyperi-io/dfe-transform-vrl/commit/2e64348d3a3889094c0964f9779a573731f28704))
* wire parallel VRL evaluation via AdaptiveWorkerPool ([506d5e9](https://github.com/hyperi-io/dfe-transform-vrl/commit/506d5e902df54ec4e0189a6781e04141a52c5cf4))

# 1.0.0 (2026-03-29)


### Bug Fixes

* add config registry registration and reload security logging ([abf2ecc](https://github.com/hyperi-io/dfe-transform-vrl/commit/abf2ecc21393e438c1c350bb889e2e2433262246))
* add dual-mode test infrastructure (remote/docker) [skip ci] ([fd5324f](https://github.com/hyperi-io/dfe-transform-vrl/commit/fd5324f37bb61ca5152dfa1605a7f1e22d1ee729))
* add Helm chart, KEDA, and integration tests [skip ci] ([703b119](https://github.com/hyperi-io/dfe-transform-vrl/commit/703b119137ed68235907577c51204da78fd578d2))
* add hot-reload via rustlib SharedConfig<HotConfig> [skip ci] ([8830c08](https://github.com/hyperi-io/dfe-transform-vrl/commit/8830c0871513227975866c1ffe23ea12ce284997))
* add KEDA contract, kafka and deployment unit tests [skip ci] ([95a0043](https://github.com/hyperi-io/dfe-transform-vrl/commit/95a00433d86151a58478f3be1e8e1f4668a073b5))
* add MemoryGuard backpressure and DfeSource topic naming [skip ci] ([aa5f1b1](https://github.com/hyperi-io/dfe-transform-vrl/commit/aa5f1b1ab1639847fd397af431cc816dbb145226))
* add VRL edge case tests, Transport trait, enrichment TODO [skip ci] ([7937bf3](https://github.com/hyperi-io/dfe-transform-vrl/commit/7937bf37548c6b529f3f52cbbf9feb4cc6c3b357))
* allow unwrap in test modules, fix clippy lints ([995270f](https://github.com/hyperi-io/dfe-transform-vrl/commit/995270fb06f9cf9596044cabbdf419d4aca059da))
* bump dependency floors to latest versions [skip ci] ([75ca93a](https://github.com/hyperi-io/dfe-transform-vrl/commit/75ca93aa6d91dbb6582cb7567ebfb85a6a30c930))
* bump hyperi-rustlib to >=1.16.7 ([42d9ffa](https://github.com/hyperi-io/dfe-transform-vrl/commit/42d9ffa1ae3d5327d2e63faa2833ef85e3876c7a))
* bump rustlib 1.16.6, wire DfeMetrics pipeline_ready/scaling/readiness [skip ci] ([a3e9616](https://github.com/hyperi-io/dfe-transform-vrl/commit/a3e9616169130744d279f916cd5a4e7e2b6ad8cc))
* clippy clean, add profiling profile, fix deny.toml [skip ci] ([9ea26f9](https://github.com/hyperi-io/dfe-transform-vrl/commit/9ea26f9a7b83c8c23de03817104a7dd6e8091bbe))
* enrichment v2 — multi-source FxHashMap engine with ArcSwap refresh ([c3a250f](https://github.com/hyperi-io/dfe-transform-vrl/commit/c3a250f0d2ed9ef587ae2b6e11f8305ac6a334dd))
* implement MVP — VRL engine, pipeline, health, metrics [skip ci] ([8abdad8](https://github.com/hyperi-io/dfe-transform-vrl/commit/8abdad8025711b68aeae25a8f8a3c09a9c28574f))
* implement VRL engine, kafka layer, pipeline, and observability [skip ci] ([400556f](https://github.com/hyperi-io/dfe-transform-vrl/commit/400556ff9db5474ab663a47ae285fd79db67fe9e))
* inline Renovate config (preset resolution broken) ([caeea51](https://github.com/hyperi-io/dfe-transform-vrl/commit/caeea51913d60cc26ef103426ec636e23d25d530))
* migrate semantic-release to single-versioning on main ([682fe48](https://github.com/hyperi-io/dfe-transform-vrl/commit/682fe487b54a2190e3c730316bb168843660c0fe))
* migrate to DFE metrics standard with rustlib 1.18.0 ([3f0c696](https://github.com/hyperi-io/dfe-transform-vrl/commit/3f0c696ec841b3fa02d5e92da184342174202adb))
* migrate to hyperi-ci, remove legacy ci submodule [skip ci] ([a59ce03](https://github.com/hyperi-io/dfe-transform-vrl/commit/a59ce031dd092150e43c5c35dfed71738ccc5a8b))
* patch 3 security vulnerabilities in transitive deps ([ef3b81a](https://github.com/hyperi-io/dfe-transform-vrl/commit/ef3b81ae256cac6f6f01b1ccc52a809c3c25eb05))
***REMOVED**** remove MSRV cap, build against latest stable [skip ci] ([aeba197](https://github.com/hyperi-io/dfe-transform-vrl/commit/aeba1978b8c8b4a8670ea94959ac8f1795b1308f))
* remove skip-ci blanket, CI is live via hyperi-ci ([4b39b44](https://github.com/hyperi-io/dfe-transform-vrl/commit/4b39b446ef962266299a99121193a718181517eb))
* restructure tests per HyperI testing standards ([a492a02](https://github.com/hyperi-io/dfe-transform-vrl/commit/a492a0237b26fc5553d226888942cd4e36e6ea5a))
* review fixes — SIGTERM, batch timeout, abort metrics, nested keys [skip ci] ([b55fcdd](https://github.com/hyperi-io/dfe-transform-vrl/commit/b55fcdd9a78bc1f5e0c8456fb59d82a0018ab9fd))
* review remediation — CSV parsing, password masking, cargo-deny, Transport traits ([3be2333](https://github.com/hyperi-io/dfe-transform-vrl/commit/3be2333de244c37d5f7c4ef1235c805f682b7a36))
* rustlib 1.16.3 remediation — ApplyFlatEnv, DfeMetrics, security, log spam [skip ci] ([0fad58c](https://github.com/hyperi-io/dfe-transform-vrl/commit/0fad58c1f1b744cf915a7276f38b0658cbfb449b))
* scaffold dfe-transform-vrl project [skip ci] ([ee171a5](https://github.com/hyperi-io/dfe-transform-vrl/commit/ee171a5e54c62c07185ebd60dbf38c74d26a7fec))
* set rust-version = 1.94, forward MSRV policy [skip ci] ([6626a43](https://github.com/hyperi-io/dfe-transform-vrl/commit/6626a436756cae6f3f2357c6b14f7ed1502a25ce))
* wire ConfigReloader and bump rustlib to 1.19.6 ([9ad9104](https://github.com/hyperi-io/dfe-transform-vrl/commit/9ad9104e6681889c5f6445e9b59770c1a0f077ce))


### Features

* add VRL enrichment tables with CSV/JSON loading and custom functions [skip ci] ([956001e](https://github.com/hyperi-io/dfe-transform-vrl/commit/956001e95b7e19bf8fd2d4fedfd27ecab0980a1e))

# [1.0.0-dev.5](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.0-dev.4...v1.0.0-dev.5) (2026-03-25)


### Bug Fixes

* add config registry registration and reload security logging ([abf2ecc](https://github.com/hyperi-io/dfe-transform-vrl/commit/abf2ecc21393e438c1c350bb889e2e2433262246))
* allow unwrap in test modules, fix clippy lints ([995270f](https://github.com/hyperi-io/dfe-transform-vrl/commit/995270fb06f9cf9596044cabbdf419d4aca059da))
* restructure tests per HyperI testing standards ([a492a02](https://github.com/hyperi-io/dfe-transform-vrl/commit/a492a0237b26fc5553d226888942cd4e36e6ea5a))
* wire ConfigReloader and bump rustlib to 1.19.6 ([9ad9104](https://github.com/hyperi-io/dfe-transform-vrl/commit/9ad9104e6681889c5f6445e9b59770c1a0f077ce))

# [1.0.0-dev.4](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.0-dev.3...v1.0.0-dev.4) (2026-03-22)


### Bug Fixes

* inline Renovate config (preset resolution broken) ([caeea51](https://github.com/hyperi-io/dfe-transform-vrl/commit/caeea51913d60cc26ef103426ec636e23d25d530))

# [1.0.0-dev.3](https://github.com/hyperi-io/dfe-transform-vrl/compare/v1.0.0-dev.2...v1.0.0-dev.3) (2026-03-21)


### Bug Fixes

* patch 3 security vulnerabilities in transitive deps ([ef3b81a](https://github.com/hyperi-io/dfe-transform-vrl/commit/ef3b81ae256cac6f6f01b1ccc52a809c3c25eb05))

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
***REMOVED**** remove MSRV cap, build against latest stable [skip ci] ([aeba197](https://github.com/hyperi-io/dfe-transform-vrl/commit/aeba1978b8c8b4a8670ea94959ac8f1795b1308f))
* remove skip-ci blanket, CI is live via hyperi-ci ([4b39b44](https://github.com/hyperi-io/dfe-transform-vrl/commit/4b39b446ef962266299a99121193a718181517eb))
* review fixes — SIGTERM, batch timeout, abort metrics, nested keys [skip ci] ([b55fcdd](https://github.com/hyperi-io/dfe-transform-vrl/commit/b55fcdd9a78bc1f5e0c8456fb59d82a0018ab9fd))
* rustlib 1.16.3 remediation — ApplyFlatEnv, DfeMetrics, security, log spam [skip ci] ([0fad58c](https://github.com/hyperi-io/dfe-transform-vrl/commit/0fad58c1f1b744cf915a7276f38b0658cbfb449b))
* scaffold dfe-transform-vrl project [skip ci] ([ee171a5](https://github.com/hyperi-io/dfe-transform-vrl/commit/ee171a5e54c62c07185ebd60dbf38c74d26a7fec))
* set rust-version = 1.94, forward MSRV policy [skip ci] ([6626a43](https://github.com/hyperi-io/dfe-transform-vrl/commit/6626a436756cae6f3f2357c6b14f7ed1502a25ce))


### Features

* add VRL enrichment tables with CSV/JSON loading and custom functions [skip ci] ([956001e](https://github.com/hyperi-io/dfe-transform-vrl/commit/956001e95b7e19bf8fd2d4fedfd27ecab0980a1e))
