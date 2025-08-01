# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Upgrade all dependencies to their latest versions ([#53]).
- Remove the `once_cell` and `async-trait` dependencies, as they are now covered by `std` ([#53]).
- Added Zookeeper 3.9.3 to the list of versions we test against ([#53]).

[#53]: https://github.com/stackabletech/tokio-zookeeper/pull/53

## [0.4.0] - 2024-05-08

### Changed

- [BREAKING] Replace `snafu::Whatever` types with `tokio_zookeeper::error::Error` ([#44]).

### Bugfixes

- Errors are now thread-safe (`Send + Sync`) again ([#44]).

## [0.3.0] - 2024-05-07

### Changed

- [BREAKING] Migrated errors from Failure to SNAFU ([#39]).
- [BREAKING] Migrated from `slog` to `tracing` ([#40]).
- Updated ZooKeeper versions we test against (now 3.9.2, 3.8.4, 3.7.2, 3.6.4, 3.5.10) ([#39]).

[#39]: https://github.com/stackabletech/tokio-zookeeper/pull/39
[#40]: https://github.com/stackabletech/tokio-zookeeper/pull/40
[#44]: https://github.com/stackabletech/tokio-zookeeper/pull/44

## [0.2.1] - 2023-02-13

### Changed

- Don't try to reconnect during exit ([#30]).

[#30]: https://github.com/stackabletech/tokio-zookeeper/pull/30

## [0.2.0] - 2023-02-10

### Highlights

tokio-zookeeper now uses futures 0.3 and Tokio 1, which means that it is
now compatible with Rust's async/await syntax!

#### Migration from 0.1.x

1. Upgrade the rest of your app to Tokio 1.x (you can use a compatibility wrapper for code such as tokio-zk that still
   uses Tokio 0.1, see
   [Cargo.toml](https://github.com/stackabletech/zookeeper-operator/blob/a682dcc3c7dc841917e968ba0e9fa9d33a4fabf5/rust/operator-binary/Cargo.toml#L22-L23)
   and
   [`WithTokio01Executor`](https://github.com/stackabletech/zookeeper-operator/blob/a682dcc3c7dc841917e968ba0e9fa9d33a4fabf5/rust/operator-binary/src/utils.rs#L6-L38)).
2. Upgrade tokio-zookeeper to v0.2.
3. Migrate async calls that thread the `ZooKeeper` instance to instead borrow it (for example,
   `zk.exists(path).and_then(|(zk, stat)| /* do stuff */);` becomes
   `let stat = zk.exists(path).await?;`).
4. Remove Tokio 0.1 and the compatibility wrapper if they are no longer required.

### Added

- Support all-or-nothing multi-operations ([#15]).

### Changed

- [BREAKING] Updated to futures 0.3 and Tokio 1, which are compatible with async/await ([#19]).

[#15]: https://github.com/stackabletech/tokio-zookeeper/pull/15
[#19]: https://github.com/stackabletech/tokio-zookeeper/pull/19
