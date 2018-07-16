Documentation for stat fields: https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#sc_zkStatStructure
Enforce path constraints? https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#ch_zkDataModel
Multi-server connect
Improve ergonomics of the API!
Create new session only when session has expired!
 + "session expired" notification
 + Herd effect mitigation from upstream ZK library?
 + Probably leave it to user to decide they want to establish new?
Watches
 + Add non-default ones
 + Hmm: https://zookeeper.apache.org/doc/r3.4.12/zookeeperProgrammers.html#sc_WatchGuarantees
   "Watches are ordered with respect to other events, other watches, and
   asynchronous replies. The ZooKeeper client libraries ensures that
   everything is dispatched in order."

   Sounds like something we have do deal with...
