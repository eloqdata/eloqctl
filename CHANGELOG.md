# Changelog

## [1.6.8](https://github.com/monographdb/eloq_waiter/compare/v1.6.7...v1.6.8) (2026-05-27)


### Bug Fixes

* auto-upgrade local state during install ([#407](https://github.com/monographdb/eloq_waiter/issues/407)) ([8a1a574](https://github.com/monographdb/eloq_waiter/commit/8a1a574096f4bcba0c15817cb48cc049317bdb49))
* rename checkpoint interval field ([#409](https://github.com/monographdb/eloq_waiter/issues/409)) ([d08933f](https://github.com/monographdb/eloq_waiter/commit/d08933ff2fca171e7252aefbfd0287071178b3b3))

## [1.6.7](https://github.com/monographdb/eloq_waiter/compare/v1.6.6...v1.6.7) (2026-05-23)


### Bug Fixes

* disable PG SSL to fix versions command connection ([#403](https://github.com/monographdb/eloq_waiter/issues/403)) ([621d1de](https://github.com/monographdb/eloq_waiter/commit/621d1de98523eb6152993942f541e8213a56b3fd))
* increase launch/status timeouts in stress test ([#402](https://github.com/monographdb/eloq_waiter/issues/402)) ([179683a](https://github.com/monographdb/eloq_waiter/commit/179683af595bd2741d8e0ed8d495e3a53ae4cd85))
* only remove step cleans up Docker containers in stress test ([#404](https://github.com/monographdb/eloq_waiter/issues/404)) ([74978d0](https://github.com/monographdb/eloq_waiter/commit/74978d08f6ce50424a019017545333c04e62fef7))
* sanitize backup_name for branch support - replace . and : with - ([#405](https://github.com/monographdb/eloq_waiter/issues/405)) ([33f226f](https://github.com/monographdb/eloq_waiter/commit/33f226fb8c7c1bcc87be6cc647a275a909323c2d))


### Code Refactoring

* split stress test into standalone script, clean up E2E ([#401](https://github.com/monographdb/eloq_waiter/issues/401)) ([0bfd8ee](https://github.com/monographdb/eloq_waiter/commit/0bfd8ee88469b202cd2862f18d76b36a911e1b7c))


### Tests

* add 30K concurrent connection stress test with redis-py ([#400](https://github.com/monographdb/eloq_waiter/issues/400)) ([34c2b63](https://github.com/monographdb/eloq_waiter/commit/34c2b637398af9cc31b8730ee3cb15abea6259f6))
* add multi-SDK e2e command stress tests with dual-client mode ([#406](https://github.com/monographdb/eloq_waiter/issues/406)) ([b5b1861](https://github.com/monographdb/eloq_waiter/commit/b5b18612d3292ce01be1c1d608ce1db56fca72a3))
* add stress test mode to E2E with maxclients=60000 ([#398](https://github.com/monographdb/eloq_waiter/issues/398)) ([e7b0194](https://github.com/monographdb/eloq_waiter/commit/e7b0194838b9654d07f6379ec0af0d1279799e32))

## [1.6.6](https://github.com/monographdb/eloq_waiter/compare/v1.6.5...v1.6.6) (2026-05-20)


### Bug Fixes

* backup from_path defaults to first deployment host instead of localhost ([#396](https://github.com/monographdb/eloq_waiter/issues/396)) ([88d07e3](https://github.com/monographdb/eloq_waiter/commit/88d07e3c9e81cbe369ad98e71b120807e4160d55))

## [1.6.5](https://github.com/monographdb/eloq_waiter/compare/v1.6.4...v1.6.5) (2026-05-20)


### Bug Fixes

* add Cargo.lock to release-please extra-files ([#393](https://github.com/monographdb/eloq_waiter/issues/393)) ([b5b3c4c](https://github.com/monographdb/eloq_waiter/commit/b5b3c4cdfa6cd2f76ca531cf880572fdd5285b7d))

## [1.6.4](https://github.com/monographdb/eloq_waiter/compare/v1.6.3...v1.6.4) (2026-05-20)


### Bug Fixes

* verify standby Redis readiness after mutations ([#390](https://github.com/monographdb/eloq_waiter/issues/390)) ([716ecac](https://github.com/monographdb/eloq_waiter/commit/716ecacffff91fd31a335701a05930e86fcdbc72))


### Documentation

* update E2E README with coverage table, cleanup old tests, remove --password ([#392](https://github.com/monographdb/eloq_waiter/issues/392)) ([0b89cae](https://github.com/monographdb/eloq_waiter/commit/0b89cae979d8ad26ac1d7b95f180f3d33e4a79ed))

## [1.6.3](https://github.com/monographdb/eloq_waiter/compare/v1.6.2...v1.6.3) (2026-05-15)


### Tests

* add failover, monitor status, log-service status to E2E ([#388](https://github.com/monographdb/eloq_waiter/issues/388)) ([bb9e57e](https://github.com/monographdb/eloq_waiter/commit/bb9e57e4255635058356b6b53f2cf025da83952a))
* expand E2E coverage — add stop/start/check/exec/upgrade/remove ([#387](https://github.com/monographdb/eloq_waiter/issues/387)) ([5d02ea7](https://github.com/monographdb/eloq_waiter/commit/5d02ea72bea8f9027878b97822caf3c492a4eb94))
* unify Docker E2E into single environment — launch once, test all ([#385](https://github.com/monographdb/eloq_waiter/issues/385)) ([258ada6](https://github.com/monographdb/eloq_waiter/commit/258ada6852f0286e7042058857a50ef1c0f7914c))

## [1.6.2](https://github.com/monographdb/eloq_waiter/compare/v1.6.1...v1.6.2) (2026-05-15)


### Documentation

* backfill missing v1.6.0 refactor entries in changelog ([#383](https://github.com/monographdb/eloq_waiter/issues/383)) ([5f52ab6](https://github.com/monographdb/eloq_waiter/commit/5f52ab6ba57c4a4651dda437c11da4d3b9bf0b00))

## [1.6.1](https://github.com/monographdb/eloq_waiter/compare/v1.6.0...v1.6.1) (2026-05-15)


### Code Refactoring

* add changelog-sections to capture refactor/perf/docs/test in release notes ([#380](https://github.com/monographdb/eloq_waiter/issues/380)) ([68c9d78](https://github.com/monographdb/eloq_waiter/commit/68c9d78400913234090e034dd1ffc6e6dda6830b))

## [1.6.0](https://github.com/monographdb/eloq_waiter/compare/v1.5.0...v1.6.0) (2026-05-15)


### Features

* add alertmanager target config and Prometheus hot reload ([#374](https://github.com/monographdb/eloq_waiter/issues/374)) ([8d9384f](https://github.com/monographdb/eloq_waiter/commit/8d9384fbb16f8f8521fbe010e9b84ab51d3b82a0))
* make alert thresholds configurable via topology YAML ([#372](https://github.com/monographdb/eloq_waiter/issues/372)) ([32cd45a](https://github.com/monographdb/eloq_waiter/commit/32cd45a54bb7ff5558508cd7417264b580ea9f18))


### Bug Fixes

* move alert_thresholds under prometheus in config hierarchy ([#375](https://github.com/monographdb/eloq_waiter/issues/375)) ([0ecdc46](https://github.com/monographdb/eloq_waiter/commit/0ecdc4656fc44eb8afe2c90d840ff7b0376596ce))


### Code Refactoring

* replace fragile shell pipelines with native Rust or simpler commands ([#376](https://github.com/monographdb/eloq_waiter/issues/376)) ([#377](https://github.com/monographdb/eloq_waiter/issues/377))
* improve production reliability and security — eliminate panics, SQL injection, hardcoded credentials ([#378](https://github.com/monographdb/eloq_waiter/issues/378))
* replace residual mono/monograph naming with eloq ([#379](https://github.com/monographdb/eloq_waiter/issues/379))

## [1.5.0](https://github.com/monographdb/eloq_waiter/compare/v1.4.0...v1.5.0) (2026-05-15)


### Features

* modernize eloqctl for EloqKV operations ([#368](https://github.com/monographdb/eloq_waiter/issues/368)) ([b8e35a4](https://github.com/monographdb/eloq_waiter/commit/b8e35a425d3479e83c0905bd8c6a23e56920fabd))


### Bug Fixes

* resolve clippy collapsible_match warnings on nightly ([#370](https://github.com/monographdb/eloq_waiter/issues/370)) ([4e88ccb](https://github.com/monographdb/eloq_waiter/commit/4e88ccbf123c6ec8f1214e47ad6ed7e1fe328095))

## [1.4.0](https://github.com/monographdb/eloq_waiter/compare/v1.3.0...v1.4.0) (2026-05-13)


### Features

* idempotent scale, export command, cluster_mode, Redis password fixes ([#366](https://github.com/monographdb/eloq_waiter/issues/366)) ([86ac3d5](https://github.com/monographdb/eloq_waiter/commit/86ac3d51a143473f6787407249d1831dbe9f095f))

## [1.3.0](https://github.com/monographdb/eloq_waiter/compare/v1.2.0...v1.3.0) (2026-05-13)


### Features

* idempotent scale, health/export/fix commands, cluster_mode ([#364](https://github.com/monographdb/eloq_waiter/issues/364)) ([0115cec](https://github.com/monographdb/eloq_waiter/commit/0115cec2e790742f14561f1e3db63284142dc01f))

## [1.2.0](https://github.com/monographdb/eloq_waiter/compare/v1.1.8...v1.2.0) (2026-05-12)


### Features

* improve task execution progress output ([#361](https://github.com/monographdb/eloq_waiter/issues/361)) ([95b7304](https://github.com/monographdb/eloq_waiter/commit/95b73044da49a83a570f06dfb7e40a6826d4856e))


### Bug Fixes

* wait for tx nodes ready before round2 failover in update-conf ([#360](https://github.com/monographdb/eloq_waiter/issues/360)) ([5fb48da](https://github.com/monographdb/eloq_waiter/commit/5fb48da6c3d00334a92b59d49b6749ce65cfb693))

## [1.1.8](https://github.com/monographdb/eloq_waiter/compare/v1.1.7...v1.1.8) (2026-05-11)


### Bug Fixes

* unpack standby nodes only after they are stopped in rolling update ([#358](https://github.com/monographdb/eloq_waiter/issues/358)) ([187b245](https://github.com/monographdb/eloq_waiter/commit/187b245bdad31eada5eff83d533f691a9c68e537))

## [1.1.7](https://github.com/monographdb/eloq_waiter/compare/v1.1.6...v1.1.7) (2026-05-11)


### Bug Fixes

* re-upload prometheus config on monitor restart and wait for tx ready in update ([#356](https://github.com/monographdb/eloq_waiter/issues/356)) ([72fcb33](https://github.com/monographdb/eloq_waiter/commit/72fcb33d00b94405492c43501d4ac1f8863edcbd))

## [1.1.6](https://github.com/monographdb/eloq_waiter/compare/v1.1.5...v1.1.6) (2026-05-11)


### Bug Fixes

* support enable_tls change in apply ([#353](https://github.com/monographdb/eloq_waiter/issues/353)) ([f6093b7](https://github.com/monographdb/eloq_waiter/commit/f6093b725c58f6955cb845bb178d529b4d79e645))

## [1.1.5](https://github.com/monographdb/eloq_waiter/compare/v1.1.4...v1.1.5) (2026-05-11)


### Bug Fixes

* support remote_write_urls in apply and fix update split_task panic ([#351](https://github.com/monographdb/eloq_waiter/issues/351)) ([d6e3881](https://github.com/monographdb/eloq_waiter/commit/d6e3881d23fe804be23efa4cdb9a12dde68a1c95))

## [1.1.4](https://github.com/monographdb/eloq_waiter/compare/v1.1.3...v1.1.4) (2026-05-11)


### Bug Fixes

* zero-downtime apply/update with correct topology discovery ([#349](https://github.com/monographdb/eloq_waiter/issues/349)) ([54ac96f](https://github.com/monographdb/eloq_waiter/commit/54ac96fadba2ecd06d38ecedd11dc5349ca060de))

## [1.1.3](https://github.com/monographdb/eloq_waiter/compare/v1.1.2...v1.1.3) (2026-05-11)


### Bug Fixes

* skip checkpoint before failover in zero-downtime apply ([#347](https://github.com/monographdb/eloq_waiter/issues/347)) ([d246169](https://github.com/monographdb/eloq_waiter/commit/d24616977aead8348be54a7a8da3e603bf0583f1))

## [1.1.2](https://github.com/monographdb/eloq_waiter/compare/v1.1.1...v1.1.2) (2026-05-11)


### Bug Fixes

* trigger v1.1.2 release build ([#345](https://github.com/monographdb/eloq_waiter/issues/345)) ([dba3693](https://github.com/monographdb/eloq_waiter/commit/dba3693a284912ff7bfc6fa8c5cffc69f68e7bdf))

## [1.1.1](https://github.com/monographdb/eloq_waiter/compare/v1.1.0...v1.1.1) (2026-05-11)


### Bug Fixes

* resolve panic in zero-downtime apply caused by duplicate topology task key ([#342](https://github.com/monographdb/eloq_waiter/issues/342)) ([4bb4fea](https://github.com/monographdb/eloq_waiter/commit/4bb4fea2e4c8362bc8334001ee8be2bbc3cc391d))

## [1.1.0](https://github.com/monographdb/eloq_waiter/compare/v1.0.6...v1.1.0) (2026-05-11)


### Features

* complete zero-downtime rolling upgrade with two-round failover ([e98110d](https://github.com/monographdb/eloq_waiter/commit/e98110dfedc937f8c438001e5cb607bcc9a14e24))
* enable apply for storage_service changes, add RocksDB tuning params and Prometheus remote_write ([#341](https://github.com/monographdb/eloq_waiter/issues/341)) ([1f1abcb](https://github.com/monographdb/eloq_waiter/commit/1f1abcba51eaedb9d9776b510b2aa94b45ca5fc5))
* remove Cassandra support and add Prometheus retention config ([#340](https://github.com/monographdb/eloq_waiter/issues/340)) ([470f72d](https://github.com/monographdb/eloq_waiter/commit/470f72d67b16a71c2e9107648d36d81c76f99981))
* show monitor status and improve eloqctl completion ([#338](https://github.com/monographdb/eloq_waiter/issues/338)) ([d4218fb](https://github.com/monographdb/eloq_waiter/commit/d4218fb2f3dd2189cd7e6b6a485b724e9f83d2ce))
* zero-downtime upgrade with failover for cluster with standby ([e3e7eb3](https://github.com/monographdb/eloq_waiter/commit/e3e7eb380241f83507e2b4533ecfb06c4f2e4f7a))

## [1.0.6](https://github.com/monographdb/eloq_waiter/compare/v1.0.5...v1.0.6) (2026-04-27)


### Bug Fixes

* **ci:** publish release assets in release workflow ([#336](https://github.com/monographdb/eloq_waiter/issues/336)) ([39502c8](https://github.com/monographdb/eloq_waiter/commit/39502c8783d42a9d99abcfcc448e808a3f244172))

## [1.0.5](https://github.com/monographdb/eloq_waiter/compare/v1.0.4...v1.0.5) (2026-04-27)


### Bug Fixes

* improve eloqctl status and quality gates ([#334](https://github.com/monographdb/eloq_waiter/issues/334)) ([008264c](https://github.com/monographdb/eloq_waiter/commit/008264cd5a07ee90af09423636f043a5e515ae25))

## [1.0.4](https://github.com/monographdb/eloq_waiter/compare/v1.0.3...v1.0.4) (2026-04-24)


### Bug Fixes

* **cluster_mgr:** use package version for eloqctl -V output ([#331](https://github.com/monographdb/eloq_waiter/issues/331)) ([9b1d409](https://github.com/monographdb/eloq_waiter/commit/9b1d40947374726d4213802d34e46d4678cb259e))

## [1.0.3](https://github.com/monographdb/eloq_waiter/compare/v1.0.2...v1.0.3) (2026-04-24)


### Bug Fixes

* **install:** robust latest resolution and idempotent profile setup ([#328](https://github.com/monographdb/eloq_waiter/issues/328)) ([893dafd](https://github.com/monographdb/eloq_waiter/commit/893dafde61a04151a8c5fc1b0af941a9580f3a34))

## [1.0.2](https://github.com/monographdb/eloq_waiter/compare/v1.0.1...v1.0.2) (2026-04-24)


### Bug Fixes

* **rest_api:** pass new verbose arg to CmdExecutor::run ([3592431](https://github.com/monographdb/eloq_waiter/commit/3592431818e6523c78540f722875d171716bc097))
