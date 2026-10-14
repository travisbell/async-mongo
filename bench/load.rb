# frozen_string_literal: true

# A closed-loop HTTP load generator: each connection sends a request, waits for
# the response, and repeats until the duration is up. Prints JSON results.
#
# Connections are opened (with a request to --connect-path) before the
# measurement starts, and all processes start measuring together, so results
# reflect serving requests on established connections rather than accepting
# a burst of new ones.
#
#   ruby bench/load.rb http://127.0.0.1:9292/find --concurrency 64 --duration 10 --processes 4

Warning[:experimental] = false

require "async"
require "async/barrier"
require "async/http/client"
require "async/http/endpoint"
require "json"
require "optparse"

module Bench
  class Load
    # How long to keep trying to open every connection before measuring anyway.
    CONNECT_TIMEOUT = 60

    def initialize(url, concurrency:, duration:, processes:, connect_path: "/ping")
      @url = url
      @concurrency = concurrency
      @duration = duration
      @processes = [processes, concurrency].min
      @connect_path = connect_path
    end

    def run
      started = monotonic_time
      results = fork_workers
      elapsed = monotonic_time - started

      latencies = results.flat_map { |result| result[:latencies] }.sort!
      errors = results.sum { |result| result[:errors] }
      requests = latencies.size
      duration = results.map { |result| result[:elapsed] }.max || elapsed

      {
        url: @url,
        concurrency: @concurrency,
        duration: duration.round(3),
        requests: requests,
        errors: errors,
        error_samples: results.flat_map { |result| result[:error_samples] }.uniq.first(5),
        connect_errors: results.sum { |result| result[:connect_errors] },
        rps: (requests / duration).round(1),
        latency_ms: latency_summary(latencies)
      }
    end

    private

    def fork_workers
      connections = Array.new(@processes) { |i| (@concurrency / @processes) + (i < @concurrency % @processes ? 1 : 0) }

      workers = connections.map do |count|
        reader, writer = IO.pipe
        ready_reader, ready_writer = IO.pipe
        go_reader, go_writer = IO.pipe

        pid = fork do
          [reader, ready_reader, go_writer].each(&:close)
          writer.write(Marshal.dump(run_worker(count, ready_writer, go_reader)))
          writer.close
          exit!(0)
        end

        [writer, ready_writer, go_reader].each(&:close)
        [pid, reader, ready_reader, go_writer]
      end

      # Start measuring once every process has opened its connections.
      workers.each { |_, _, ready, _| ready.read(1) }
      workers.each do |_, _, ready, go|
        ready.close
        go.write("G")
        go.close
      end

      workers.map do |pid, reader|
        data = reader.read
        reader.close
        Process.wait(pid)
        Marshal.load(data) # Produced by our own forked worker.
      end
    end

    def run_worker(connections, ready, go)
      endpoint = Async::HTTP::Endpoint.parse(@url)
      path = endpoint.path
      latencies = []
      errors = 0
      error_samples = []
      connect_errors = 0
      started = nil

      Sync do
        clients = Array.new(connections) { Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP11, limit: 1) }
        connect_errors = open_connections(clients)

        ready.write("R")
        ready.close
        go.read(1)
        go.close

        started = monotonic_time
        deadline = started + @duration
        barrier = Async::Barrier.new

        clients.each do |client|
          barrier.async do
            while monotonic_time < deadline
              request_started = monotonic_time

              begin
                response = client.get(path)
                response.read

                if response.status == 200
                  latencies << monotonic_time - request_started
                else
                  errors += 1
                  error_samples << "HTTP #{response.status}" if error_samples.size < 5
                end
              rescue StandardError => e
                errors += 1
                error_samples << "#{e.class}: #{e.message}" if error_samples.size < 5
                sleep(0.01)
              end
            end
          ensure
            client&.close
          end
        end

        barrier.wait
      end

      { latencies: latencies, errors: errors, error_samples: error_samples, connect_errors: connect_errors, elapsed: monotonic_time - started }
    end

    # Make one request on each client so its connection is open (retrying, as a
    # busy server may be slow to accept a burst of connections).
    #
    # @returns [Integer] Clients that couldn't connect in time.
    def open_connections(clients)
      deadline = monotonic_time + CONNECT_TIMEOUT
      failed = 0
      barrier = Async::Barrier.new

      clients.each do |client|
        barrier.async do
          loop do
            response = client.get(@connect_path)
            response.read
            break if response.status == 200

            raise "HTTP #{response.status}"
          rescue StandardError
            if monotonic_time > deadline
              failed += 1
              break
            end

            sleep(0.05)
          end
        end
      end

      barrier.wait
      failed
    end

    def latency_summary(latencies)
      return {} if latencies.empty?

      percentile = ->(p) { (latencies[[(latencies.size * p).ceil - 1, 0].max] * 1000).round(2) }

      {
        mean: (latencies.sum / latencies.size * 1000).round(2),
        p50: percentile.(0.50),
        p90: percentile.(0.90),
        p99: percentile.(0.99),
        max: (latencies.last * 1000).round(2)
      }
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end

if $PROGRAM_NAME == __FILE__
  options = { concurrency: 16, duration: 10.0, processes: 4 }

  OptionParser.new do |parser|
    parser.banner = "Usage: ruby bench/load.rb URL [options]"
    parser.on("-c", "--concurrency N", Integer, "Concurrent connections (default 16)") { |v| options[:concurrency] = v }
    parser.on("-d", "--duration SECONDS", Float, "Duration (default 10)") { |v| options[:duration] = v }
    parser.on("-p", "--processes N", Integer, "Load generator processes (default 4)") { |v| options[:processes] = v }
    parser.on("--connect-path PATH", "Request used to open each connection before measuring (default /ping)") { |v| options[:connect_path] = v }
  end.parse!

  url = ARGV.fetch(0) { abort("Usage: ruby bench/load.rb URL [options]") }
  puts JSON.generate(Bench::Load.new(url, **options).run)
end
