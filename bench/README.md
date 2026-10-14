# Benchmarks

The same Rack app ([app.rb](app.rb)) on Puma's thread pool and Falcon's
fibers, with the MongoDB driver's stock connection pool and with async-mongo.
One command runs it and writes an HTML report:

```bash
bundle install
bundle exec ruby bench/run.rb          # ~50 minutes
bundle exec ruby bench/run.rb --quick  # 3s runs, ~20 minutes
open bench/results/<timestamp>.html
```

Needs a MongoDB server (`MONGODB_URI`, default
`mongodb://localhost:27017/async_mongo_bench`); it seeds 10,000 documents.

## Suites

The default suite, `concurrency`, compares Puma's thread pool with Falcon's
fibers as the number of concurrent clients grows (1, 4, 16, 64, 256 and
1,024), at 1 process and at 4 processes:

| Setup | 1 process | 4 processes |
|---|---|---|
| Puma + stock mongo, 4, 8 and 16 threads | `puma-1x4`, `puma-1x8`, `puma-1x16` | `puma-4x4`, `puma-4x8`, `puma-4x16` |
| Falcon + stock mongo | `falcon-stock-1` | `falcon-stock-4` |
| Falcon + async-mongo | `falcon-1` | `falcon-4` |

Puma gets a database connection per thread, as Ruby apps usually do, so its
concurrency is its thread count. Falcon runs a fiber per request with 64
connections per process. The report plots requests/s, p99 latency and CPU
against the number of clients (Puma's thread counts as shades and dash
patterns of one color), and its headline numbers compare against the best
Puma configuration at each point.

Optional suites (`--suites concurrency,connections,attribution,ceiling`):

| Suite | What it shows |
|---|---|
| `connections` | Puma with 4, 8, 16 and 64 threads against Falcon + async-mongo with the same number of connections per process (`falcon-1-pool4`, …), at 1 and 4 processes and 64 and 256 clients: the same database concurrency, so the difference is fibers and the driver |
| `attribution` | One Falcon process with neither, either or both parts of async-mongo, at 256 clients |
| `ceiling` | `/ping` (no database) on Puma and Falcon, the HTTP servers' own ceiling |

Each server is started once and measured for everything any selected suite
needs from it, so shared measurements are taken once.

## Servers

Server names describe the configuration:

| Name | Meaning |
|---|---|
| `puma-WxT` | Puma, W workers × T threads, stock pool. `puma-1x16` is single mode (no cluster master). |
| `puma-async-mongo-WxT` | The same with async-mongo installed (every checkout uses its fallback stock pool) |
| `falcon-N` | Falcon with N worker processes and async-mongo |
| `falcon-stock-N` | Falcon with N workers and the stock pool |
| `falcon-pool-only-N` | `Async::Mongo.install!(socket: false)` |
| `falcon-socket-only-N` | `Async::Mongo.install!(pool: false)` |
| `…-poolN` suffix | Connection pool size per process, e.g. `falcon-1-pool16`, `puma-1x64-pool16` |

Each process gets `max_pool_size` equal to its thread count under Puma and 64
under Falcon (override with `--pool-size`). Reports show the total connections
across processes.

For a one-off comparison, pass any of `--servers`, `--scenarios` and
`--concurrency`:

```bash
bundle exec ruby bench/run.rb --servers puma-2x8,falcon-2 --scenarios remote_fanout --concurrency 1,64 --latency-ms 20
bundle exec ruby bench/run.rb --help
```

## Scenarios

| Scenario | Request |
|---|---|
| `/find` | one indexed `find_one` against the local database |
| `/find?remote=1` | the same, through [latency_proxy.rb](latency_proxy.rb), which delays the database's replies by `--latency-ms` (default 5ms) to simulate a database farther away than localhost |
| `/fanout?remote=1` | 5 `find_one`s per request through the proxy: concurrent under Falcon (`Async::Barrier`), one after another under Puma, as each would naturally be written |
| `/fanout` | the same against the local database (custom runs) |
| `/ping` | no database (`ceiling` suite) |

The load generator ([load.rb](load.rb)) is closed-loop: N persistent HTTP/1.1
connections, each sending its next request when the previous response arrives.

When queries wait on the network (`remote*`), throughput is capped at about
`connections / round trip` for any server: with 16 connections and ~6ms round
trips, ~2,700 queries/s. Raising it means more connections, which for Puma
means more threads or workers. Concurrency 1 shows per-request latency without
any queueing.

## Output

- `bench/results/<timestamp>.html`: the report. Headline numbers, a chart and
  a table per scenario (requests/s, p50/p90/p99 latency, errors, CPU, memory,
  connections, threads), the method and environment, and the raw data. It's a
  single self-contained file (no external assets), in light and dark mode.
- `bench/results/<timestamp>.json`: the raw results. Regenerate the HTML from
  it with `bundle exec ruby bench/run.rb --report bench/results/<timestamp>.json`.
- `tmp/<server>.log`: each server's output.

## Fairness notes

- Puma gets a connection per thread (4, 8 or 16); Falcon 64 per process.
- CPU is the server's total across its processes (100% = one core) and memory
  is the resident size of all of them; Falcon and clustered Puma run a
  supervisor/master process next to their workers.
- The load generator (4 processes by default, `--load-processes`) and usually
  MongoDB run on the same machine. On a laptop the database is much closer than
  in production, which is what the `remote` scenarios compensate for, and at
  high request rates the database (not the app server) can become the limit;
  compare with `ping`.
- YJIT is on for the servers (`--no-yjit` to turn it off).

## Profiling

[profile.rb](profile.rb) profiles the driver's hot path in-process (concurrent
`find_one`s from Async tasks, no HTTP) with stackprof:

```bash
bundle exec ruby bench/profile.rb --mode both    # or none, pool, socket
bundle exec ruby bench/profile.rb --mode both --out tmp/both.prof
```
