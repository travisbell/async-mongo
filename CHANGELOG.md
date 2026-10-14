# Changelog

## 0.1.0 (unreleased)

- `Async::Mongo.install!` opts the MongoDB driver into:
  - `Async::Mongo::ConnectionPool`: an event-loop native connection pool, with a
    stock-pool fallback for checkouts outside the owner thread's event loop,
    and first-come-first-served waiting.
  - `Async::Mongo::SocketExtension`: socket waits through the fiber scheduler
    instead of `Kernel.select` (which Async runs on a new thread per call).
  - `Async::Mongo::SafeFinalizer`: a stock-pool finalizer that can't raise.
- Behaves like `Mongo::Server::ConnectionPool` (events, close reasons, errors,
  clear/pause/populate/close semantics); passes all 32 official CMAP spec tests.
- Puma vs Falcon benchmark in `bench/`, with an HTML report.
