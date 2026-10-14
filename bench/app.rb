# frozen_string_literal: true

# The Rack app both servers run. Configured with environment variables:
#
#   MONGODB_URI        Database to use (default mongodb://localhost:27017/async_mongo_bench).
#   MONGODB_REMOTE_URI The same database through bench/latency_proxy.rb, for ?remote=1.
#   MONGO_POOL_SIZE    max_pool_size for each server (default 16).
#   ASYNC_MONGO        "1" to install async-mongo, "pool" or "socket" to install only that part.
#   SERVER_MODEL       Label returned by /stats ("puma", "falcon", ...).

Warning[:experimental] = false

require "json"
require "rack"
require "mongo"
require "async"
require "async/barrier"

case ENV["ASYNC_MONGO"]
when "1"
  require "async/mongo"
  Async::Mongo.install!
when "pool"
  require "async/mongo"
  Async::Mongo.install!(socket: false)
when "socket"
  require "async/mongo"
  Async::Mongo.install!(pool: false)
end

Mongo::Logger.logger.level = Logger::WARN

module Bench
  DOCUMENTS = 10_000
  FANOUT = 5

  class App
    def initialize
      @client = nil
      @client_lock = Mutex.new
    end

    def call(env)
      case env["PATH_INFO"]
      when "/ping" then text("ok")
      when "/find" then json(find(collection(env), rand(DOCUMENTS)))
      when "/fanout" then json(fanout(collection(env)))
      when "/stats" then json(stats)
      else [404, { "content-type" => "text/plain" }, ["not found"]]
      end
    end

    private

    def client
      # Created lazily so that each worker process gets its own (after any fork).
      @client || @client_lock.synchronize do
        @client ||= build_client(ENV.fetch("MONGODB_URI", "mongodb://localhost:27017/async_mongo_bench"))
      end
    end

    # The same database through the latency proxy, as if it were farther away.
    def remote_client
      @remote_client || @client_lock.synchronize do
        @remote_client ||= build_client(ENV.fetch("MONGODB_REMOTE_URI"))
      end
    end

    def build_client(uri)
      Mongo::Client.new(uri, max_pool_size: Integer(ENV.fetch("MONGO_POOL_SIZE", "16")), wait_queue_timeout: 30)
    end

    def collection(env)
      (query_param(env, "remote") == "1" ? remote_client : client)[:documents]
    end

    # A fast indexed lookup.
    def find(documents, id)
      documents.find(_id: id).projection(body: 0).first
    end

    # Several independent lookups for one request: concurrent under Async
    # (Falcon), sequential otherwise (Puma), as each would naturally be written.
    def fanout(documents)
      ids = Array.new(FANOUT) { rand(DOCUMENTS) }

      if Async::Task.current?
        barrier = Async::Barrier.new
        tasks = ids.map { |id| barrier.async { find(documents, id) } }
        barrier.wait
        tasks.map(&:wait)
      else
        ids.map { |id| find(documents, id) }
      end
    end

    def stats
      clients = [@client, @remote_client].compact
      pools = clients.flat_map(&:cluster).flat_map(&:servers).map do |server|
        pool = server.pool_internal
        next unless pool

        pool.respond_to?(:stats) ? pool.stats : { size: pool.size, available: pool.available_count }
      end

      {
        server: ENV.fetch("SERVER_MODEL", "unknown"),
        # Async::Mongo::VERSION is always loaded (Bundler evaluates the gemspec), so check for install! itself.
        async_mongo: Async::Mongo.respond_to?(:installed?) && Async::Mongo.installed?,
        threads: Thread.list.size,
        pools: pools.compact
      }
    end

    def query_param(env, name)
      query = env["QUERY_STRING"]
      return if query.nil? || query.empty?

      Rack::Utils.parse_query(query)[name]
    end

    def text(body)
      [200, { "content-type" => "text/plain" }, [body]]
    end

    def json(object)
      [200, { "content-type" => "application/json" }, [JSON.generate(object)]]
    end
  end
end
