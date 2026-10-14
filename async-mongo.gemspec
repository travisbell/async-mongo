# frozen_string_literal: true

require_relative "lib/async/mongo/version"

Gem::Specification.new do |spec|
  spec.name = "async-mongo"
  spec.version = Async::Mongo::VERSION
  spec.authors = ["Travis Bell"]
  spec.email = ["me@travisbell.com"]

  spec.summary = "Lets Falcon's fiber concurrency reach MongoDB: a fiber-native connection pool and socket I/O for the MongoDB Ruby driver."
  spec.description = <<~DESCRIPTION
    The MongoDB Ruby driver was built for threads: under Falcon and Async every
    socket wait starts a thread and the connection pool runs on threads, so
    Falcon ends up no faster than Puma. async-mongo makes the driver
    fiber-native, opt-in and without forking it: socket waits go through the
    event loop and a fiber-native connection pool (a drop-in replacement that
    passes the official CMAP spec tests) replaces the stock one, falling back
    to the stock pool outside the event loop.
  DESCRIPTION
  spec.homepage = "https://github.com/travisbell/async-mongo"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "README.md", "CHANGELOG.md", "LICENSE.txt"]
  spec.require_paths = ["lib"]

  spec.add_dependency("async", ">= 2.24", "< 3")
  spec.add_dependency("mongo", ">= 2.24", "< 3")
end
