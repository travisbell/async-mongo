# frozen_string_literal: true

require "async/queue"
require "yaml"

# Runs the official CMAP (Connection Monitoring and Pooling) spec tests against
# Async::Mongo::ConnectionPool, adapted from the MongoDB Ruby driver's runner
# (spec/runners/cmap.rb at v2.26.0). The specs' "threads" run as Async tasks in
# one reactor, which is how the pool is used.
#
# CMAP_POOL=stock runs them against Mongo::Server::ConnectionPool instead, to
# check the runner.
#
# Needs a MongoDB server: MONGODB_URI, default
# mongodb://localhost:27017. Specs that use fail points also need the server
# to allow test commands (mongod --setParameter enableTestCommands=1) and are
# skipped otherwise.
module CmapSpec
  # Collects published CMAP events.
  class Subscriber
    attr_reader :events

    def initialize
      @events = []
    end

    def published(event)
      @events << event
    end
  end

  # One spec file.
  class Spec
    attr_reader :description, :pool_options, :expected_error, :expected_events, :ignore_events, :fail_point, :subscriber, :pool

    def initialize(path)
      @test = YAML.safe_load_file(path)
      @description = @test["description"]
      @pool_options = process_options(@test["poolOptions"])
      @operations = @test["operations"].map { |operation| Operation.new(self, operation) }
      @expected_error = @test["error"]
      @expected_events = @test["events"]
      @ignore_events = @test["ignore"] || []
      @fail_point = @test["failPoint"]
      @run_on = @test["runOn"] || []
    end

    # @returns [String | Nil] Why the spec can't run against this server, if it can't.
    def unsatisfied_reason(server_version:, topology:, test_commands:)
      @run_on.each do |requirement|
        case requirement.keys.first
        when "minServerVersion"
          return "needs server #{requirement.values.first}+" if Gem::Version.new(server_version) < Gem::Version.new(requirement.values.first)
        when "maxServerVersion"
          return "needs server <= #{requirement.values.first}" if Gem::Version.new(server_version) > Gem::Version.new(requirement.values.first)
        when "topology"
          return "needs topology #{requirement.values.first.join("/")}" unless requirement.values.first.include?(topology)
        end
      end

      "needs test commands (fail points)" if @fail_point && !test_commands
    end

    def setup(pool, subscriber, client)
      @pool = pool
      @subscriber = subscriber
      @client = client
      @client.database.command(@fail_point) if @fail_point
    end

    def run
      state = {}
      result = { "error" => nil }

      Sync do
        @operations.each do |operation|
          error = operation.run(@pool, state)
          if error
            result["error"] = error
            break
          end
        end

        # Taken before stopping leftover tasks, like the driver's runner does before killing its threads.
        result["events"] = @subscriber.events.filter_map do |event|
          hash = event_hash(event)
          hash unless @ignore_events.include?(hash.fetch("type"))
        end
      ensure
        state.each_value { |value| value.stop if value.is_a?(Async::Task) }
      end

      result
    ensure
      @client.database.command(configureFailPoint: @fail_point["configureFailPoint"], mode: "off") if @fail_point
    end

    private

    def event_hash(event)
      cmap = Mongo::Monitoring::Event::Cmap

      case event
      when cmap::PoolCreated then { "type" => "ConnectionPoolCreated", "address" => event.address, "options" => normalize_options(event.options) }
      when cmap::PoolClosed then { "type" => "ConnectionPoolClosed", "address" => event.address }
      when cmap::ConnectionCreated then { "type" => "ConnectionCreated", "connectionId" => event.connection_id, "address" => event.address }
      when cmap::ConnectionReady then { "type" => "ConnectionReady", "connectionId" => event.connection_id, "address" => event.address }
      when cmap::ConnectionClosed then { "type" => "ConnectionClosed", "connectionId" => event.connection_id, "reason" => event.reason, "address" => event.address }
      when cmap::ConnectionCheckOutStarted then { "type" => "ConnectionCheckOutStarted", "address" => event.address }
      when cmap::ConnectionCheckOutFailed then { "type" => "ConnectionCheckOutFailed", "reason" => event.reason, "address" => event.address }
      when cmap::ConnectionCheckedOut then { "type" => "ConnectionCheckedOut", "connectionId" => event.connection_id, "address" => event.address }
      when cmap::ConnectionCheckedIn then { "type" => "ConnectionCheckedIn", "connectionId" => event.connection_id, "address" => event.address }
      when cmap::PoolCleared then { "type" => "ConnectionPoolCleared", "address" => event.address, "interruptInUseConnections" => event.options[:interrupt_in_use_connections] }
      when cmap::PoolReady then { "type" => "ConnectionPoolReady", "address" => event.address }
      else raise "Unhandled event: #{event}"
      end
    end

    # The driver's option names, in the spec's format.
    def normalize_options(options)
      (options || {}).each_with_object({}) do |(key, value), normalized|
        case key
        when :max_idle_time then normalized["maxIdleTimeMS"] = (value * 1000.0).to_i
        when :max_size then normalized["maxPoolSize"] = value
        when :min_size then normalized["minPoolSize"] = value
        when :wait_queue_size then normalized["waitQueueSize"] = value
        when :wait_timeout then normalized["waitQueueTimeoutMS"] = (value * 1000.0).to_i
        end
      end
    end

    # The spec's option names, in the driver's format.
    def process_options(options)
      (options || {}).each_with_object({}) do |(key, value), processed|
        case key
        when "maxIdleTimeMS" then processed[:max_idle_time] = value / 1000.0
        when "maxPoolSize" then processed[:max_pool_size] = value
        when "minPoolSize" then processed[:min_pool_size] = value
        when "waitQueueSize" then processed[:wait_queue_size] = value
        when "waitQueueTimeoutMS" then processed[:wait_queue_timeout] = value / 1000.0
        when "maxConnecting" then processed[:max_connecting] = value
        when "appName" then processed[:app_name] = value
        when "backgroundThreadIntervalMS" then nil # The populator doesn't poll.
        else raise "Unknown pool option #{key}"
        end
      end
    end
  end

  # One operation. Operations with a `thread` are queued to that task.
  class Operation
    attr_reader :name

    def initialize(spec, operation)
      @spec = spec
      @name = operation["name"]
      @thread = operation["thread"]
      @target = operation["target"]
      @ms = operation["ms"]
      @label = operation["label"]
      @connection = operation["connection"]
      @event = operation["event"]
      @count = operation["count"]
      @interrupt_in_use_connections = !!operation["interruptInUseConnections"]
    end

    def run(pool, state, main = true)
      return run_on_task(state) if @thread && main

      case @name
      when "start" then start(pool, state)
      when "ready" then pool.ready
      when "wait" then sleep(@ms / 1000.0)
      when "waitForThread" then wait_for_task(state)
      when "waitForEvent" then wait_for_event
      when "checkOut" then check_out(pool, state)
      when "checkIn" then check_in(pool, state)
      when "clear" then clear(pool)
      when "close" then pool.close
      else raise "Unknown operation #{@name}"
      end

      nil
    # Errors are compared by type and these fixed messages, like the driver's runner does.
    rescue Mongo::Error::PoolClosedError
      raise unless main

      { "type" => "PoolClosedError", "message" => "Attempted to check out a connection from closed connection pool" }
    rescue Mongo::Error::ConnectionCheckOutTimeout
      raise unless main

      { "type" => "WaitQueueTimeoutError", "message" => "Timed out while checking out a connection from connection pool" }
    end

    private

    def start(pool, state)
      queue = Async::Queue.new

      task = Async(finished: false) do
        while (operation = queue.dequeue)
          operation.run(pool, state, false)
        end
      end

      task.define_singleton_method(:operations) { queue }
      state[@target] = task
      sleep(0.1)
    end

    def run_on_task(state)
      task = state[@thread] or raise "No thread #{@thread}"
      task.operations.enqueue(self)
      # Let the task run the operation.
      sleep(0.1)
      nil
    end

    def wait_for_task(state)
      task = state[@target] or raise "No thread #{@target}"
      task.operations.enqueue(nil)
      # Raises the task's error, if any (converted to the spec format by #run).
      task.wait
    end

    def wait_for_event
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
      name = @event.sub(/\AConnectionPool/, "Pool")

      loop do
        count = @spec.subscriber.events.count { |event| event.class.name.split("::").last.sub(/\AConnectionPool/, "Pool") == name }
        break if count >= @count
        raise "Didn't get #{@count} #{@event} events (got #{count})" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        sleep(0.01)
      end
    end

    def check_out(pool, state)
      connection = pool.check_out
      state[@label] = connection if @label
    end

    def check_in(pool, state)
      sleep(0.01) until state[@connection]
      pool.check_in(state[@connection])
    end

    def clear(pool)
      # Like the driver's runner: the server is unknown when the pool is cleared.
      server = pool.server
      server.define_singleton_method(:unknown?) { true }
      pool.clear(lazy: true, interrupt_in_use_connections: @interrupt_in_use_connections)
    ensure
      server.singleton_class.send(:remove_method, :unknown?)
    end
  end
end

RSpec.describe("CMAP spec tests") do
  uri = ENV.fetch("MONGODB_URI", "mongodb://localhost:27017")
  probe = nil
  server_info = begin
    probe = Mongo::Client.new(uri, server_selection_timeout: 2, connect_timeout: 2, database: "admin")
    server = probe.cluster.next_primary
    test_commands = begin
      probe.database.command(configureFailPoint: "failCommand", mode: "off")
      true
    rescue Mongo::Error::OperationFailure
      false
    end

    topology = case probe.cluster.topology
    when Mongo::Cluster::Topology::Single then "single"
    when Mongo::Cluster::Topology::Sharded then "sharded"
    else "replicaset"
    end

    { address: server.address, description: server.description, version: probe.database.command(buildInfo: 1).first["version"], topology: topology, test_commands: test_commands }
  rescue Mongo::Error
    nil
  end

  after(:all) { probe&.close }

  Dir[File.join(__dir__, "cmap", "*.yml")].sort.each do |path|
    spec = CmapSpec::Spec.new(path)

    context("#{spec.description} (#{File.basename(path)})") do
      before(:all) do
        skip("no MongoDB at #{uri}") unless server_info

        reason = spec.unsatisfied_reason(server_version: server_info[:version], topology: server_info[:topology], test_commands: server_info[:test_commands])
        skip(reason) if reason

        subscriber = CmapSpec::Subscriber.new
        monitoring = Mongo::Monitoring.new(monitoring: false)
        monitoring.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)

        cluster = Struct.new(:options, :app_metadata) do
          def topology = nil
          def run_sdam_flow(*) = nil
          def update_cluster_time(*) = nil
          def cluster_time = nil
        end.new({}, Mongo::Server::AppMetadata.new(spec.pool_options.slice(:app_name)))

        options = { monitoring_io: false, populator_io: true }.merge(probe.options.slice(:user, :password, :auth_source, :auth_mech, :ssl, :ssl_verify)).merge(spec.pool_options)
        @server = Mongo::Server.new(server_info[:address], cluster, monitoring, Mongo::Event::Listeners.new, options)

        description = server_info[:description]
        @server.define_singleton_method(:description) { description }
        # The cluster is a stand-in, so marking the server unknown doesn't clear
        # the pool through SDAM; do what SDAM would (like the driver's runner).
        @server.define_singleton_method(:unknown!) do |*|
          define_singleton_method(:unknown?) { true }
          pool_internal&.clear(lazy: true)
        end

        # CMAP_POOL=stock runs the same specs against the driver's pool, to check the runner itself.
        pool_class = ENV["CMAP_POOL"] == "stock" ? Mongo::Server::ConnectionPool : Async::Mongo::ConnectionPool
        pool = pool_class.new(@server, @server.options)
        @server.instance_variable_set(:@pool, pool)

        spec.setup(pool, subscriber, probe)
        @result = spec.run
      end

      after(:all) do
        next unless @server

        Sync { spec.pool&.close }
      end

      it "raises the expected error" do
        expect(@result["error"]).to eq(spec.expected_error)
      end

      it "emits the expected events" do
        actual = @result["events"].map do |event|
          event = event.dup
          event["reason"] = event["reason"].to_s.gsub(/_[a-z]/) { |m| m[1].upcase } if event["reason"]
          event
        end

        normalized = actual.zip(spec.expected_events).map do |event, expected|
          next event unless event && expected

          # 42 in the spec means "any value".
          event = event.slice(*expected.keys)
          expected.each { |key, value| event[key] = 42 if value == 42 && event[key] }
          event
        end

        expect(normalized).to eq(spec.expected_events)
      end
    end
  end
end
