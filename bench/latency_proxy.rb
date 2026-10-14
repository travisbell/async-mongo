# frozen_string_literal: true

# A TCP proxy that delays everything the upstream server sends by a fixed
# amount, simulating a database that's farther away (each request/response
# round trip takes DELAY_MS longer). Throughput isn't limited: data is
# timestamped as it arrives and released when it's due.
#
#   ruby bench/latency_proxy.rb LISTEN_PORT UPSTREAM_HOST:PORT DELAY_MS

require "async"
require "async/queue"
require "socket"

module Bench
  class LatencyProxy
    def initialize(port, upstream_host, upstream_port, delay)
      @port = port
      @upstream_host = upstream_host
      @upstream_port = upstream_port
      @delay = delay
    end

    def run
      Sync do
        server = TCPServer.new("127.0.0.1", @port)
        $stdout.puts("listening on #{@port}")
        $stdout.flush

        loop do
          client = server.accept
          Async { proxy(client) }
        end
      end
    end

    private

    def proxy(client)
      upstream = TCPSocket.new(@upstream_host, @upstream_port)
      [client, upstream].each { |socket| socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1) }

      requests = Async { copy(client, upstream) }
      delayed_copy(upstream, client)
      requests.stop
    rescue IOError, SystemCallError
      # A side went away.
    ensure
      client.close unless client.closed?
      upstream&.close unless upstream&.closed?
    end

    def copy(from, to)
      while (data = from.readpartial(65_536))
        to.write(data)
      end
    rescue EOFError, IOError, SystemCallError
      to.close_write rescue nil
    end

    def delayed_copy(from, to)
      queue = Async::Queue.new

      writer = Async do
        while (due, data = queue.dequeue) && data
          wait = due - monotonic_time
          sleep(wait) if wait > 0
          to.write(data)
        end
      end

      begin
        while (data = from.readpartial(65_536))
          queue.enqueue([monotonic_time + @delay, data])
        end
      rescue EOFError, IOError, SystemCallError
        queue.enqueue([monotonic_time + @delay, nil])
      end

      writer.wait
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end

if $PROGRAM_NAME == __FILE__
  port, upstream, delay_ms = ARGV
  abort("Usage: ruby bench/latency_proxy.rb LISTEN_PORT UPSTREAM_HOST:PORT DELAY_MS") unless port && upstream && delay_ms

  host, upstream_port = upstream.split(":")
  Bench::LatencyProxy.new(Integer(port), host, Integer(upstream_port), Float(delay_ms) / 1000).run
end
