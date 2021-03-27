The new threading model introduced since 0.8.0 has some basic goals
1. Avoid dropping UDP packets as much as possible
2. Distribute blocking counting work between sync threads
3. Distribute asynchronous work, like timers or TCP-based networking using a single async runtime

# Thread pools
The most important thing for processing UDP traffic is to avoid blocking any thread
that does UDP processing. Because in such a case especially under heavy ingress traffic the thread's queue
may be filled almost instantly. This will lead to filling OS queue for UDP socket and OS will start
dropping UDP packets.

To avoid blocking situations, UDP processing is moved to 2 separate thread pools:

* UDP networking thread pool.
    Contains blocking synchronous threads, responsible for listening UDP sockets and periodically flushing the
    collected raw data to parsing threads.

* "fast" UDP parsing thread pool.
    These threads are also blocking and are responsible for parsing raw data and accumulating such data inrelatively small
    local batches. These batches(we call them snapshots) are flushed relatively often(around few seconds) and sent out of the threads for being
    aggregated into a big global batch and/or be sent to remote nodes. The flushing is done fast and has a higher priority than parsing.

    To maintain consistent parsing and precise snapshot taking, each thread in this pool has it's own channel and  may be reached individually.


All other tasks, not sensitive to blocking as UDP, are distributed between 2 other pools, which should be familiar
to anyone who develops multithreading applications these days:

* "Slow" general purpose thread pool.
   As it was said, the "slow" threads are responsible for making all other tasks, that could block for a relatively long
   time or require a heavy calculations to be done. The examples of tasks good for this pool are accumulation of metrics
   in some global hashmap and parallel aggregate calculation from this pool when time comes.

   Their "slow" threads' main difference from the "fast" ones is the single task queue, which they process alltogether.
   So, for example, if one of the threads blocks on lock or a heavy calculation like aggregation of a big timer metric,
   other threads are still getting other tasks from the queue, alowing it to empty faster and doing work in parallel.

* Async thread pool.
    This one is basically a standard async runtime(based on `tokio` library) able to process M:N scheduling for N green threads.
    This makes it perfect for processing timer-based and I/O-bound processing of everything our daemon needs. All TCP clients, servers,
    periodic tasks are starting here, sometimes offloading their heavy work to slow synchronous pool.

There also some additional threads, existing because of implementation details, which are not important here.

# Configuration
A number of threads for each thread pool can be set in config by using the parameters below.
Each parameter's default is 4, and may be set to 0 to be detected automatically from number of CPU cores (not recommended).

Every user has it's own different use case and hardware for their load, which means there is no best out of the box configuration
for number of threads in each pool. Still, obviously, for UDP-heavy traffic n-threads and p-threads should prevail. But for
agent-heavy infrastructures and high cadrinality metrics it's more valuable to increase the a-threads and w-threads numbers leaving UDP low.

We recommend experimenting on these parameters watching each thread's load in `top` or similar utilities and tuning them accordingly.

* `n-threads` for networking threads
* `p-threads` for "fast" parsing threads
* `w-threads` for sync "slow" threads
* `a-threads` for async threads. (please note, that despite the async threads' low load, it is not receommended to set their amount to a value
  lower than 4 to avoid strange locks or delays in their activity)
