**aggregation** - a process of joining metrics gathered through some time period using a formula or an algorithm to some,
usually, single value

**node** - an instance of bioyino

**leader node** - a node chosen(by consensus or manually) to be the one who sends metrics to backends

**consensus** - subsystem responsible for selecting leader node between many connected nodes

**snapshot** - a pack of metrics received during a short period (usually an order of magnitude shorter than 
aggregation period) used for data redundancy

**backend** - a remote endpoint supporting receiving of metrics in Carbon format

**Carbon** - network protocol and metric format used by Graphite

