# frozen_string_literal: true

# Shorthand for `require "async/mongo"; Async::Mongo.install!`, e.g. in a Gemfile:
#
#   gem "async-mongo", require: "async/mongo/install"
require_relative "../mongo"

Async::Mongo.install!
