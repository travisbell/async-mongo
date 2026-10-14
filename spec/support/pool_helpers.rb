# frozen_string_literal: true

# Fakes for unit testing the pool without a MongoDB server.
module PoolHelpers
  class FakeDescription
    def initialize(unknown: false)
      @unknown = unknown
    end

    def unknown?
      @unknown
    end

    def service_id
      nil
    end
  end

  class FakeServer
    attr_reader :address, :options, :monitoring, :unknown_calls
    attr_accessor :description

    def initialize(address: "localhost:27017")
      @address = address
      @options = {}
      @monitoring = nil
      @description = FakeDescription.new
      @unknown_calls = []
    end

    def load_balancer?
      false
    end

    def unknown?
      @description.unknown?
    end

    def unknown!(options = {})
      @unknown_calls << options
      @description = FakeDescription.new(unknown: true)
    end

    def recover!
      @description = FakeDescription.new
    end
  end

  # A real Async::Mongo::Connection (so the pool recognizes it and the real
  # reusable?/viable? logic runs), with the I/O replaced by flags.
  class FakeConnection < Async::Mongo::Connection
    @next_id = 0

    class << self
      def next_id
        @next_id += 1
      end
    end

    attr_reader :id, :disconnect_reasons
    attr_accessor :error_flag, :interrupted_flag

    def initialize(pool:, generation:)
      # Deliberately not calling super: no server, no socket.
      @id = self.class.next_id
      @options = { connection_pool: pool, generation: generation }
      @pid = Process.pid
      @socket = Object.new
      @closed = false
      @error_flag = false
      @interrupted_flag = false
      @disconnect_reasons = []
    end

    def closed?
      @closed
    end

    def error?
      @error_flag
    end

    def interrupted?
      @interrupted_flag
    end

    def pinned?
      false
    end

    def service_id
      nil
    end

    def description
      nil
    end

    def record_checkin!
      self
    end

    def disconnect!(options = nil)
      options ||= {}
      @disconnect_reasons << options[:reason]
      @interrupted_flag = true if options[:interrupted]
      @socket = nil
      @closed = true
    end
  end

  def fake_server
    @fake_server ||= FakeServer.new
  end

  # Build a pool whose connections are FakeConnections. The block, if given,
  # runs before each connection is created (e.g. to sleep or raise).
  def build_pool(**options, &before_connect)
    pool = Async::Mongo::ConnectionPool.new(fake_server, { monitoring: false }.merge(options))

    allow(pool).to receive(:create_connection) do
      before_connect&.call
      raise Mongo::Error::PoolClosedError.new(pool.address, pool) if pool.closed?
      raise Mongo::Error::PoolClearedError.new(pool.address, pool) unless pool.ready?

      FakeConnection.new(pool: pool, generation: pool.generation)
    end

    pool
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
