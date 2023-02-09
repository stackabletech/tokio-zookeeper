# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
