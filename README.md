# async-mongo

Lets [Falcon](https://github.com/socketry/falcon)'s fiber concurrency reach
MongoDB.

Puma serves concurrent requests with a pool of threads, one request per
thread. Falcon runs each request in a fiber on
[Async](https://github.com/socketry/async)'s event loop, so one process can
serve many requests at once without a thread for each, and a request can run
several queries concurrently. But the
[MongoDB Ruby driver](https://github.com/mongodb/mongo-ruby-driver) was built
for threads, and under Falcon it gives most of that back:

- **Every socket wait starts a thread.** `Mongo::Socket` waits with
  `Kernel.select`, which under a fiber scheduler goes through `io_select`, and
  Async implements that by running `IO.select` on a new thread. The driver
  waits on nearly every read, so each query costs a thread, and Falcon ends up
  no faster than Puma, with worse tail latency under load.
- **The pool wasn't built for fibers.** Waiting for a connection, establishing
  one and maintaining the pool all go through threads (a populator per server),
  and a fiber that's cancelled mid-query (a client disconnects, a
  `with_timeout` fires) returns its connection with the unread reply still on
  the socket, so the next query on it gets someone else's reply
  (`Mongo::Error::UnexpectedResponse`).

async-mongo makes the driver fiber-native without forking it: socket waits go
through the event loop, and a connection pool that runs on fibers replaces the
stock one. It changes nothing until you call `Async::Mongo.install!`, and it's
a drop-in replacement: it passes all of the official MongoDB connection pool
spec tests.

When queries wait on the network, one Falcon process with async-mongo serves
2.6–2.7× the requests of a 16-thread Puma process, and four Falcon processes
serve 7–39% more requests than four Puma processes from 64 clients up, usually
on far less CPU (as little as 40% of Puma's). Falcon with the stock driver gets
some of that concurrency, but its latency falls apart under load. See
[Benchmarks](#benchmarks).

## Installation

```ruby
# Gemfile
gem "async-mongo", require: "async/mongo/install"
```

or install explicitly at boot, before any `Mongo::Client` connects:

```ruby
require "async/mongo"

Async::Mongo.install!                 # pool + socket extension (where supported)
Async::Mongo.install!(socket: false)  # just the pool
Async::Mongo.install!(pool: false)    # just the socket extension
```

`install!` is process-wide, idempotent, and can't be undone. It checks that the
driver still has the internals async-mongo relies on and raises
`Async::Mongo::IncompatibleDriverError` (naming what's missing) if not.

It's safe to install in any process: connections are only taken from the
async pool by fibers running in an Async task; everything else (Puma threads,
Sidekiq, scripts) transparently uses the driver's stock pool.

## What it changes

### `Async::Mongo::ConnectionPool`

Replaces `Mongo::Server::ConnectionPool` for every server, except in
load-balanced deployments (see [Load-balanced deployments](#load-balanced-deployments)).

- **Event-loop native.** Checkout, check-in and waiting never block the
  reactor. Waiting for a connection, establishing one and maintaining the pool
  all happen on fibers.
- **Cancellation safe.** If anything other than a `Mongo::Error` (e.g.
  `Async::Stop`, `Timeout::Error`) interrupts an operation, the connection is
  discarded instead of returned to the pool.
- **No pool threads.** A transient maintenance fiber keeps `min_pool_size`
  connections, retires idle (`max_idle_time`) and stale connections, and
  interrupts in-use connections when the driver detects that the server went
  away (so fibers blocked on a dead server get a retryable `PoolClearedError`).
  The driver's own background threads only flip flags and wake it.
- **Fast checkouts.** Idle connections are handed out without waiting behind
  connections being established; at most `max_connecting` connections are
  established at once, and they count toward `max_pool_size`. The most
  recently used connection is reused first.
- **Fork aware.** Connections inherited from a parent process are never reused.
- **Thread-safe fallback.** The pool is owned by one thread: the first that
  checks out from an Async task. Checkouts from other threads, or from outside
  an Async task, go to a stock pool created on first use, which shares the
  async pool's generation counter so the driver's server monitoring stays
  consistent. In a forked Falcon worker, every request uses the async pool.

See [Sizing the connection pool](#sizing-the-connection-pool) for how to
choose `max_pool_size` under Falcon.

It honors the driver's pool options: `max_pool_size`, `min_pool_size`,
`wait_queue_timeout`, `max_idle_time`, `max_connecting`, and client-side
operation timeouts (`timeoutMS`). It publishes the standard CMAP monitoring
events, and `pool.stats` returns a summary:

```ruby
client.cluster.servers.first.pool.stats
# => {state: :ready, idle: 7, in_use: 1, connecting: 0, max_size: 20, min_size: 0, maintenance: true, fallback: nil}
```

`in_use` is a snapshot, so a sampler polling it misses short bursts.
`pool.take_peak_in_use` returns the most connections in use at once since its
last call. Tracking starts with the first call, so a pool that's never measured
doesn't do it.

### `Async::Mongo::SocketExtension`

Prepended to `Mongo::Socket`. When a fiber scheduler is active, reads and
write-waits use `IO#wait_readable` / `IO#wait_writable`, which the scheduler
handles in its event loop; otherwise (e.g. the driver's monitor threads) the
stock code runs. Connection establishment still uses `IO.select`; it only
happens when a pool opens a connection.

The stock read also watches a pipe that lets the pool wake readers blocked on a
dead server. The extension relies on closing the socket instead, which wakes a
fiber blocked on it only on Ruby 3.5+ (`Fiber::Scheduler#fiber_interrupt`). So
`install!` enables it automatically only there; on Ruby 3.4 you can force it
with `install!(socket: true)`, and readers on a dead server are then only woken
by `socketTimeoutMS`.

### `Async::Mongo::SafeFinalizer`

Installed with the pool. The stock pool's finalizer stops at the first
connection that fails to disconnect (e.g. `Errno::EBADF` after a fork, or an
`Interrupt`), and an exception escaping a finalizer surfaces in whatever code
the GC interrupted. This one disconnects each connection independently and
never raises.

## Benchmarks

`bundle exec ruby bench/run.rb` compares Puma + stock mongo, Falcon + stock
mongo and Falcon + async-mongo as the number of concurrent clients grows from 1
to 1,024, at 1 process and at 4 processes. It writes a self-contained HTML
report plotting requests/s, latency and CPU against the number of clients; see
[bench/](bench/README.md).

Puma runs with 4, 8 or 16 threads and a database connection per thread, as Ruby
apps usually do, so its concurrency is its thread count. Falcon runs a fiber
per request with 64 connections per process, so its concurrency is its
connection pool. Measured on an Apple M3 Ultra, Ruby 4.0.6 + YJIT, MongoDB 8.0
on localhost (Docker), mongo 2.26.0, Puma 8.0.2, Falcon 0.57.0. Requests/s:

| 1 process | Clients | Puma, 4 threads | Puma, 8 threads | Puma, 16 threads | Falcon + stock mongo | Falcon + async-mongo |
|---|--:|--:|--:|--:|--:|--:|
| `/find` | 64 | 5,472 | 5,310 | 5,349 | 5,506 | **8,344** |
| `/find` | 256 | 5,423 | 5,493 | 5,399 | 4,690 | **7,536** |
| `/find?remote=1` | 64 | 615 | 1,363 | 2,793 | 5,103 | **7,647** |
| `/find?remote=1` | 256 | 620 | 1,321 | 2,806 | 4,470 | **7,302** |
| `/fanout?remote=1` | 64 | 124 | 259 | 542 | 909 | **1,548** |
| `/fanout?remote=1` | 256 | 123 | 264 | 536 | 689 | **1,492** |

| 4 processes | Clients | Puma, 4 threads | Puma, 8 threads | Puma, 16 threads | Falcon + stock mongo | Falcon + async-mongo |
|---|--:|--:|--:|--:|--:|--:|
| `/find` | 64 | 10,048 | 10,185 | 10,490 | 10,211 | **11,901** |
| `/find` | 256 | 9,737 | 10,749 | 10,688 | 11,295 | **13,741** |
| `/find?remote=1` | 64 | 2,619 | 5,099 | 9,275 | 9,090 | **9,908** |
| `/find?remote=1` | 256 | 2,627 | 5,593 | 10,017 | 10,097 | **11,387** |
| `/fanout?remote=1` | 64 | 526 | 949 | 1,778 | 1,989 | **2,473** |
| `/fanout?remote=1` | 256 | 527 | 1,130 | 2,116 | 2,082 | **2,476** |

`/find` is one indexed query against the local database; `?remote=1` adds 5ms
to every database round trip (a database farther away than localhost), and
`/fanout` runs 5 queries per request, concurrently in fibers under Falcon and
one after another under Puma.

- **Puma's throughput is capped by its threads when queries wait on the
  network.** With the database 5ms away, one Puma process serves about 620,
  1,340 or 2,800 req/s with 4, 8 or 16 threads, however many clients are
  waiting. One Falcon + async-mongo process serves 7,300–7,650 (2.6–2.7× a
  16-thread Puma), and with five queries per request 2.8–2.9× as many.
- **Fibers alone aren't enough.** Falcon with the stock driver gets past
  Puma's thread cap too, but falls off as load grows, with tail latency
  measured in hundreds of milliseconds to seconds (p99 317ms at 256 clients
  against 40ms with async-mongo, one process, 5ms away).
- **On a local database, the work is CPU-bound** and Puma's thread count barely
  matters. One Falcon + async-mongo process serves 37–58% more requests than
  the best Puma configuration from 16 to 256 clients; four processes serve
  13–28% more on about 40% of the CPU (2.3–2.7 cores against 5.1–6.5).
- **At four processes, with the database 5ms away**, Falcon + async-mongo
  serves 7–22% more requests than a 16-thread Puma (64 connections in all) from
  64 clients up, on about half its CPU (2.1–2.4 cores against 3.7–4.6); Puma
  with 8 or 4 threads serves about a half or a quarter as many.
- **One Falcon process has limits**: it takes every request at once, each in a
  fiber, on one core, so at 1,024 clients a single process falls behind Puma
  on the local database (4,497 against 5,584 req/s). With four processes it
  stays ahead. Run several Falcon workers, as you would Puma workers.
- **Tail latency at four processes** is comparable to Puma's at 64 and 1,024
  clients, but at 256 clients Falcon's p99 was higher in this run (46ms
  against 34–40ms on `/find`; 77ms against 33ms with the database 5ms away,
  which reran at 34–40ms against 29–37ms), though its median was lower (18ms
  against 25ms; 20ms against 26ms). That's the database, not the server: 64
  connections per worker let four Falcon processes drive this MongoDB to its
  limit, where about 74 queries are in flight in MongoDB at once (6 under
  Puma), and they queue there while it uses about 10 cores. With 16
  connections per worker, the same 64 in all as Puma, Falcon serves 21% more
  than Puma with a lower p99 (12,745 req/s at 29.5ms against 10,527 at
  37.6ms) and less than half the database CPU. See
  [Sizing the connection pool](#sizing-the-connection-pool).
- **Where the gains come from** (one process, `/find`, 256 clients): the socket
  extension alone takes Falcon from 4,690 to 7,001 req/s, the pool alone cuts
  p99 from 126ms to 72ms, and together they give 7,536 req/s at 41ms p99.
- Around 13,000–14,000 req/s the local MongoDB is the limit; without a
  database (`/ping`), four Falcon processes serve about 150,000 req/s against
  Puma's 88,000.

### Same number of connections

With the same number of database connections per process (Puma with a thread
per connection), the database concurrency is equal and the difference is
fibers and the driver. Requests/s at 256 clients, Puma → Falcon + async-mongo:

| Connections per process | `/find`, 1 process | `/find?remote=1`, 1 process | `/fanout?remote=1`, 1 process | `/find?remote=1`, 4 processes |
|--:|--:|--:|--:|--:|
| 4 | 5,423 → **6,610** | **620** → 560 | **123** → 109 | **2,627** → 2,545 |
| 8 | 5,493 → **6,962** | **1,321** → 1,233 | **264** → 231 | **5,593** → 5,406 |
| 16 | 5,399 → **7,050** | **2,806** → 2,593 | **536** → 451 | 10,017 → **10,495** |
| 64 | 5,399 → **7,536** | 5,081 → **7,302** | 1,275 → **1,492** | 9,181 → **11,387** |

- **CPU-bound work is faster on Falcon + async-mongo at every pool size**:
  22–40% on one process and 9–37% on four, at 256 clients.
- **When the connections are the limit, it's close to a tie**: with 4–16
  connections and the database 5ms away, throughput is about connections ÷
  round trip on either server, and Puma is level or slightly ahead (by up to
  16% with five queries per request on one process), with Falcon using the
  same CPU or less.
- **The difference is what more connections cost.** For Puma each one is a
  thread, and at 64 threads one process is held back by the GVL: 5,081 req/s
  on 1.3 cores, against Falcon's 7,302 on 1.0 core (44% more); at four
  processes, 9,181 req/s on 6.4 cores against 11,387 on 2.1. Fibers make
  connections cheap, so Falcon can run enough of them to keep up with a
  database that's a few milliseconds away.

Numbers vary between runs by a few percent; the report has every
measurement, including p50/p90/p99 latency, CPU and memory.

## Sizing the connection pool

Under Puma, the thread count limits how many queries a process runs at once,
whatever `max_pool_size` says. Under Falcon, every request has its own fiber,
so `max_pool_size` is that limit, and it should be sized for the database
rather than for the number of clients:

- Each process needs about **queries per second × query round trip**
  connections: a process doing 2,000 queries/s against a database 5ms away
  keeps about 10 busy; on localhost at 0.5ms, about 1.
- Leave headroom for bursts, but more connections than the database can serve
  at once don't add throughput. They queue inside the database, raising its
  CPU and your tail latency. In the benchmarks, four Falcon processes with 64
  connections each kept about 74 queries in flight in a local MongoDB and used
  about 10 of its cores; with 16 each, they served only 8% fewer requests on
  less than half the database CPU, with a lower p99.
- Count connections across all processes and servers: that total is what the
  database sees.
- The driver's default `max_pool_size` is 20 per server; requests beyond it
  wait in the pool's queue (`wait_queue_timeout`, 10s by default), first come
  first served.

## Compatibility

| | |
|---|---|
| Ruby | 3.4+ (socket extension auto-enabled on 3.5+) |
| mongo | 2.24 – 2.26 tested; checked for compatibility at install |
| async | 2.24+ |
| MongoDB topologies | standalone, replica set, sharded; [load-balanced](#load-balanced-deployments) servers keep the stock pool |

`Async::Mongo::ConnectionPool` is a drop-in replacement for
`Mongo::Server::ConnectionPool`: the same public methods and options, the same
CMAP monitoring events (in the same order, with the same `ConnectionClosed`
reasons), the same errors, and the same semantics for clearing, pausing,
populating and closing.

- **Spec compliance:** it passes all 32 of the official
  [CMAP spec tests](https://github.com/mongodb/specifications/tree/master/source/connection-monitoring-and-pooling)
  (as vendored by the Ruby driver), run with the specs' "threads" as Async
  tasks. The same runner passes the stock pool, which checks the runner
  itself (`CMAP_POOL=stock`).
- **Failure handling:** integration specs check, through the driver and with
  fail points, that network and state-change errors clear the pool and retries
  succeed, that ordinary command errors leave it alone, and that blocked
  connections time out waiting checkouts and then recover.

The pool's state is only touched by its owner thread's fibers. In Falcon's
threaded or hybrid modes, the first reactor thread uses the async pool and the
others use the fallback stock pool.

### Load-balanced deployments

When the driver connects in load-balanced mode (`loadBalanced=true`, e.g. a
MongoDB Atlas sharded cluster behind a private endpoint, or mongos behind a
layer 4 load balancer), servers keep the driver's stock
`Mongo::Server::ConnectionPool`. Everything works exactly as it does without
async-mongo, but the async pool's improvements don't apply to those
connections.

In this mode the load balancer may route each connection to a different
mongos, so the driver pins a connection to a cursor (for its `getMore`s) or to
a transaction until it finishes, tracks connection generations per backend
service, and clears the pool per service. Supporting that in the async pool
needs connection-specific checkouts, waiting for a particular pinned connection
to be returned, and per-service clearing and interruption, all of which need a
load-balanced test environment to verify. The stock pool already implements it
correctly.

| | Other topologies | Load-balanced |
|---|---|---|
| Connection pool | `Async::Mongo::ConnectionPool` | `Mongo::Server::ConnectionPool` |
| Socket waits through the fiber scheduler (`SocketExtension`) | ✓ | ✓ |
| Safe stock-pool finalizer (`SafeFinalizer`) | ✓ (fallback pool) | ✓ |
| Connections discarded when a fiber is cancelled mid-operation | ✓ | ✗ a connection can be returned with an unread reply, and the next operation on it fails |
| Fair (first come, first served) waiting with one wakeup per release | ✓ | stock behavior |
| Pool maintenance (`min_pool_size`, idle and stale connections) | maintenance fiber | populator thread per server |
| In-use connections interrupted when the server goes away | ✓ | ✓ (by the populator thread) |

To check which pool a server uses:

```ruby
client.cluster.servers.map { |server| server.pool.class }
# => [Mongo::Server::ConnectionPool] in load-balanced mode
```

## Development

```bash
bundle install
docker run -d --rm --name async-mongo-test -p 27017:27017 mongo:8 --setParameter enableTestCommands=1
bundle exec rspec          # MONGODB_URI, default mongodb://localhost:27017/async_mongo_test
bundle exec rubocop
MONGO_RUBY_DRIVER_VERSION="~> 2.24.0" bundle update mongo && bundle exec rspec
```

The CMAP and failure specs need a server with test commands enabled (for fail
points) and are skipped without one; the integration specs need any MongoDB.

## License

MIT
