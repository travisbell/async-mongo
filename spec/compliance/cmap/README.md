# CMAP spec tests

These YAML files are the connection pool tests from the MongoDB driver
specifications ([Connection Monitoring and Pooling](https://github.com/mongodb/specifications/tree/master/source/connection-monitoring-and-pooling)),
as vendored by the MongoDB Ruby driver at v2.26.0
(`spec/spec_tests/data/cmap`). They're licensed under the Apache License 2.0
by MongoDB, Inc.

`spec/compliance/cmap_spec.rb` runs them against `Async::Mongo::ConnectionPool`,
with the specs' "threads" running as Async tasks.
