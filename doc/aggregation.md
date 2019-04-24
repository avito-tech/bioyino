# Aggregation modes
Depending of aggregation heaviness, there may be different ways to perform it.

The most notable parameter here is the size of and array for a single metric. There may be a lot of i.e. `ms`-typed
metrics, but when only few metrics come during the aggregation period, counting stats for all of them is fast and one
thread will most probably be enough.

## Single threaded
In this mode the whole array of metrics is counted in a single thread. Usually this is enough for a low-sized batches
i.e. when only a few metrics with the same name are received during aggregation period. Another use case is when
aggregation time is not important and it is ok to wait some time leaving all other cores for processing.

## Common pool multithreaded
Can be enabled by setting `aggregation-mode` to "common" in `metrics` section.

The aggregation is distributed between same worker threads, that do parsing and initial metric processing.
Only one thread is started to join the results received from these workers.

The biggest disadvantage of this approach is that aggregation of some big metric packs can block worker threads, so for
some time they could not process incoming data and therefore UDP drops may increase.

## Separate pool multithreaded
Can be enabled by setting `aggregation-mode` to "separate" in `metrics` section.

This mode runs a totally separate thread pool which will try to aggregate all the metrics as fast as possible.
To set the size of the pool `aggregaton-threads` option can be used. It only works in "separate" mode. If set to 0,
the number of threads will be taken automatically by number of cpu cores.
