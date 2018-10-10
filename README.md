# tokio-zookeeper

[![Crates.io](https://img.shields.io/crates/v/tokio-zookeeper.svg)](https://crates.io/crates/tokio-zookeeper)
[![Documentation](https://docs.rs/tokio-zookeeper/badge.svg)](https://docs.rs/tokio-zookeeper/)
[![Build Status](https://travis-ci.org/jonhoo/tokio-zookeeper.svg?branch=master)](https://travis-ci.org/jonhoo/tokio-zookeeper)

This crate provides a client for interacting with [Apache
ZooKeeper](https://zookeeper.apache.org/), a highly reliable distributed service for
maintaining configuration information, naming, providing distributed synchronization, and
providing group services.

## About ZooKeeper

The [ZooKeeper Overview](https://zookeeper.apache.org/doc/current/zookeeperOver.html) provides
a thorough introduction to ZooKeeper, but we'll repeat the most important points here. At its
[heart](https://zookeeper.apache.org/doc/current/zookeeperOver.html#sc_designGoals), ZooKeeper
is a [hierarchical key-value
store](https://zookeeper.apache.org/doc/current/zookeeperOver.html#sc_dataModelNameSpace) (that
is, keys can have "sub-keys"), which additional mechanisms that guarantee consistent operation
across client and server failures. Keys in ZooKeeper look like paths (e.g., `/key/subkey`), and
every item along a path is called a
"[Znode](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_zkDataModel_znodes)".
Each Znode (including those with children) can also have associated data, which can be queried
and updated like in other key-value stores. Along with its data and children, each Znode stores
meta-information such as [access-control
lists](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl),
[modification
timestamps](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_timeInZk),
and a version number
that allows clients to avoid stepping on each other's toes when accessing values (more on that
later).

### Operations

ZooKeeper's API consists of the same basic operations you would expect to find in a
file-system: [`create`](struct.ZooKeeper.html#method.create) for creating new Znodes,
[`delete`](struct.ZooKeeper.html#method.delete) for removing them,
[`exists`](struct.ZooKeeper.html#method.exists) for checking if a node exists,
[`get_data`](struct.ZooKeeper.html#method.get_data) and
[`set_data`](struct.ZooKeeper.html#method.set_data) for getting and setting a node's associated
data respectively, and [`get_children`](struct.ZooKeeper.html#method.get_children) for
retrieving the children of a given node (i.e., its subkeys). For all of these operations,
ZooKeeper gives [strong
guarantees](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkGuarantees)
about what happens when there are multiple clients interacting with the system, or even what
happens in response to system and network failures.

### Ephemeral nodes

When you create a Znode, you also specify a [`CreateMode`]. Nodes that are created with
[`CreateMode::Persistent`] are the nodes we have discussed thus far. They remain in the server
until you delete them. Nodes that are created with [`CreateMode::Ephemeral`] on the other hand
are special. These [ephemeral
nodes](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#Ephemeral+Nodes) are
automatically deleted by the server when the client that created them disconnects. This can be
handy for implementing lease-like mechanisms, and for detecting faults. Since they are
automatically deleted, and nodes with children cannot be deleted directly, ephemeral nodes are
not allowed to have children.

### Watches

In addition to the methods above, [`ZooKeeper::exists`], [`ZooKeeper::get_data`], and
[`ZooKeeper::get_children`] also support setting
"[watches](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches)" on
a node. A watch is one-time trigger that causes a [`WatchedEvent`] to be sent to the client
that set the watch when the state for which the watch was set changes. For example, for a
watched `get_data`, a one-time notification will be sent the first time the data of the target
node changes following when the response to the original `get_data` call was processed. You
should see the ["Watches" entry in the Programmer's
Guide](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches) for
details.

### Getting started

To get ZooKeeper up and running, follow the official [Getting Started
Guide](https://zookeeper.apache.org/doc/current/zookeeperStarted.html). In most Linux
environments, the procedure for getting a basic setup working is usually just to install the
`zookeeper` package and then run `systemctl start zookeeper`. ZooKeeper will then be running at
`127.0.0.1:2181`.

## This implementation

This library is analogous to the asynchronous API offered by the [official Java
implementation](https://zookeeper.apache.org/doc/current/api/org/apache/zookeeper/ZooKeeper.html),
and for most operations the Java documentation should apply to the Rust implementation. If this
is not the case, it is considered [a bug](https://github.com/jonhoo/tokio-zookeeper/issues),
and we'd love a bug report with as much relevant information as you can offer.

Note that since this implementation is asynchronous, users of the client must take care to
not re-order operations in their own code. There is some discussion of this in the [official
documentation of the Java
bindings](https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#Java+Binding).

For more information on ZooKeeper, see the [ZooKeeper Programmer's
Guide](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html) and the [Confluence
ZooKeeper wiki](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Index). There is also a
basic tutorial (that uses the Java client)
[here](https://zookeeper.apache.org/doc/current/zookeeperTutorial.html).

### Interaction with Tokio

The futures in this crate expect to be running under a `tokio::Runtime`. In the common case,
you cannot resolve them solely using `.wait()`, but should instead use `tokio::run` or
explicitly create a `tokio::Runtime` and then use `Runtime::block_on`.

## A somewhat silly example

```rust
extern crate tokio;
#[macro_use]
extern crate failure;
extern crate tokio_zookeeper;

use tokio_zookeeper::*;
use tokio::prelude::*;

tokio::run(
    ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap())
        .and_then(|(zk, default_watcher)| {
            // let's first check if /example exists. the .watch() causes us to be notified
            // the next time the "exists" status of /example changes after the call.
            zk.watch()
                .exists("/example")
                .inspect(|(_, stat)| {
                    // initially, /example does not exist
                    assert_eq!(stat, &None)
                })
                .and_then(|(zk, _)| {
                    // so let's make it!
                    zk.create(
                        "/example",
                        &b"Hello world"[..],
                        Acl::open_unsafe(),
                        CreateMode::Persistent,
                    )
                })
                .inspect(|(_, ref path)| {
                    assert_eq!(path.as_ref().map(String::as_str), Ok("/example"))
                })
                .and_then(|(zk, _)| {
                    // does it exist now?
                    zk.watch().exists("/example")
                })
                .inspect(|(_, stat)| {
                    // looks like it!
                    // note that the creation above also triggered our "exists" watch!
                    assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
                })
                .and_then(|(zk, _)| {
                    // did the data get set correctly?
                    zk.get_data("/example")
                })
                .inspect(|(_, res)| {
                    let data = b"Hello world";
                    let res = res.as_ref().unwrap();
                    assert_eq!(res.0, data);
                    assert_eq!(res.1.data_length as usize, data.len());
                })
                .and_then(|(zk, res)| {
                    // let's update the data.
                    zk.set_data("/example", Some(res.unwrap().1.version), &b"Bye world"[..])
                })
                .inspect(|(_, stat)| {
                    assert_eq!(stat.unwrap().data_length as usize, "Bye world".len());
                })
                .and_then(|(zk, _)| {
                    // create a child of /example
                    zk.create(
                        "/example/more",
                        &b"Hello more"[..],
                        Acl::open_unsafe(),
                        CreateMode::Persistent,
                    )
                })
                .inspect(|(_, ref path)| {
                    assert_eq!(path.as_ref().map(String::as_str), Ok("/example/more"))
                })
                .and_then(|(zk, _)| {
                    // it should be visible as a child of /example
                    zk.get_children("/example")
                })
                .inspect(|(_, children)| {
                    assert_eq!(children, &Some(vec!["more".to_string()]));
                })
                .and_then(|(zk, _)| {
                    // it is not legal to delete a node that has children directly
                    zk.delete("/example", None)
                })
                .inspect(|(_, res)| assert_eq!(res, &Err(error::Delete::NotEmpty)))
                .and_then(|(zk, _)| {
                    // instead we must delete the children first
                    zk.delete("/example/more", None)
                })
                .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                .and_then(|(zk, _)| zk.delete("/example", None))
                .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                .and_then(|(zk, _)| {
                    // no /example should no longer exist!
                    zk.exists("/example")
                })
                .inspect(|(_, stat)| assert_eq!(stat, &None))
                .and_then(move |(zk, _)| {
                    // now let's check that the .watch().exists we did in the very
                    // beginning actually triggered!
                    default_watcher
                        .into_future()
                        .map(move |x| (zk, x))
                        .map_err(|e| format_err!("stream error: {:?}", e.0))
                })
                .inspect(|(_, (event, _))| {
                    assert_eq!(
                        event,
                        &Some(WatchedEvent {
                            event_type: WatchedEventType::NodeCreated,
                            keeper_state: KeeperState::SyncConnected,
                            path: String::from("/example"),
                        })
                    );
                })
        })
        .map(|_| ())
        .map_err(|e| panic!("{:?}", e)),
);
```

# Live-coding

The crate is under development as part of a live-coding stream series
intended for users who are already somewhat familiar with Rust, and who
want to see something larger and more involved be built. For
futures-related stuff, I can also highly recommend @aturon's in-progress
[Async in Rust
book](https://aturon.github.io/apr/async-in-rust/chapter.html).

You can find the recordings of past sessions in [this YouTube
playlist](https://www.youtube.com/playlist?list=PLqbS7AVVErFgY2faCIYjJZv_RluGkTlKt).
This crate started out in [this
video](https://www.youtube.com/watch?v=mMuk8Rn9HBg), and got fleshed out
more in [this follow-up](https://www.youtube.com/watch?v=0-Fsu-aM0_A), before
we mostly finished it in [part 3](https://www.youtube.com/watch?v=1ADDeB9rqAI).
I recommend you also take a look at the [ZooKeeper Programming
Guide](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html) if
you want to follow along. To get updates about future streams, follow me on
[Patreon](https://www.patreon.com/jonhoo) or
[Twitter](https://twitter.com/jonhoo).

# Thank you

For each of the projects I build, I like to thank the people who are
willing and able to take the extra step of supporting me in making these
videos on [Patreon](https://www.patreon.com/jonhoo) or
[Liberapay](https://liberapay.com/jonhoo/). You have my most sincere
gratitude, and I'm so excited that you find what I do interesting enough
that you're willing to give a stranger money to do something they love!

 - Rodrigo Valin
 - Pigeon F
 - Patrick Allen
 - Matthew Knight
