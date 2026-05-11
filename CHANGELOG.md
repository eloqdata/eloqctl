# Changelog

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
