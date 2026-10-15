# frozen_string_literal: true

RSpec.describe(Async::Mongo::ConnectionPool) do
  describe "#initialize" do
    it "starts paused" do
      pool = build_pool
      expect(pool.state).to eq(:paused)
    end

    it "accepts the driver's option aliases" do
      pool = build_pool(min_pool_size: 1, max_pool_size: 4, wait_queue_timeout: 3)

      expect([pool.min_size, pool.max_size, pool.wait_timeout]).to eq([1, 4, 3])
    end

    it "raises if min_size exceeds max_size" do
      expect { build_pool(min_size: 10, max_size: 5) }.to raise_error(ArgumentError, /Cannot have min size/)
    end
  end

  describe "#check_out and #check_in" do
    it "reuses connections" do
      pool = build_pool
      pool.ready

      Sync do
        a = pool.check_out
        pool.check_in(a)
        b = pool.check_out

        expect(b).to equal(a)
        expect(a.connection_pool).to equal(pool)
        pool.check_in(b)
      end
    end

    it "raises PoolClosedError when closed and PoolClearedError when paused" do
      pool = build_pool

      Sync do
        expect { pool.check_out }.to raise_error(Mongo::Error::PoolClearedError)
        pool.close
        expect { pool.check_out }.to raise_error(Mongo::Error::PoolClosedError)
      end
    end

    it "times out after wait_timeout" do
      pool = build_pool(max_size: 1, wait_timeout: 0.05)
      pool.ready

      Sync do
        held = pool.check_out
        expect { pool.check_out }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout)
        pool.check_in(held)
      end
    end

    it "uses the remaining client-side operation timeout when there is one" do
      pool = build_pool(max_size: 1, wait_timeout: 10)
      pool.ready
      context = instance_double(Mongo::Operation::Context, remaining_timeout_sec: 0.05)

      Sync do
        held = pool.check_out

        started = monotonic_time
        expect { pool.check_out(context: context) }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout)
        expect(monotonic_time - started).to be < 1

        pool.check_in(held)
      end
    end

    it "tracks sizes" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out
        expect([pool.size, pool.available_count, pool.unavailable_connections]).to eq([1, 0, 1])

        pool.check_in(connection)
        expect([pool.size, pool.available_count, pool.unavailable_connections]).to eq([1, 1, 0])
        expect(pool.stats).to include(idle: 1, in_use: 0, state: :ready, fallback: nil)
      end
    end

    it "tracks the peak in use once asked" do
      pool = build_pool
      pool.ready
      pool.take_peak_in_use

      Sync do
        connections = 2.times.map { pool.check_out }
        connections.each { |connection| pool.check_in(connection) }
      end

      expect(pool.take_peak_in_use).to eq(2)
      expect(pool.take_peak_in_use).to eq(0)
    end
  end

  describe "#with_connection" do
    it "checks the connection in after the block" do
      pool = build_pool
      pool.ready

      Sync do
        pool.with_connection { |connection| expect(pool.unavailable_connections).to eq(1) }
        expect(pool.unavailable_connections).to eq(0)
        expect(pool.available_count).to eq(1)
      end
    end

    it "discards the connection when the block raises something other than a Mongo::Error" do
      pool = build_pool
      pool.ready

      Sync do
        expect { pool.with_connection { raise "boom" } }.to raise_error(RuntimeError, "boom")
        expect(pool.unavailable_connections).to eq(0)
        expect(pool.size).to eq(0)
      end
    end

    it "raises the original error when establishing the connection fails" do
      pool = build_pool { raise Mongo::Error::SocketError, "connection refused" }
      pool.ready

      Sync do
        expect { pool.with_connection { |_connection| } }.to raise_error(Mongo::Error::SocketError, "connection refused")
        expect(pool.size).to eq(0)
      end
    end

    it "turns socket errors on interrupted connections into PoolClearedError, keeping labels" do
      pool = build_pool
      pool.ready

      Sync do
        error = Mongo::Error::SocketError.new("connection reset").tap { |e| e.add_label("RetryableWriteError") }

        expect do
          pool.with_connection do |connection|
            connection.interrupted_flag = true
            raise error
          end
        end.to raise_error(Mongo::Error::PoolClearedError) { |e| expect(e.labels).to include("RetryableWriteError") }

        expect(pool.unavailable_connections).to eq(0)
      end
    end

    it "re-raises socket errors on connections that weren't interrupted" do
      pool = build_pool
      pool.ready

      Sync do
        expect { pool.with_connection { raise Mongo::Error::SocketError, "reset" } }.to raise_error(Mongo::Error::SocketError, "reset")
      end
    end

    it "disconnects a connection whose fiber was cancelled mid-operation" do
      pool = build_pool
      pool.ready

      Sync do
        used = nil
        task = Async do
          pool.with_connection do |connection|
            used = connection
            sleep
          end
        end

        sleep 0.01
        task.stop
        task.wait

        expect(used.disconnect_reasons).to include(:error)
        expect(pool.size).to eq(0)
        expect(pool.check_out).not_to equal(used)
      end
    end

    it "keeps connections after Mongo errors, which are raised after a complete reply" do
      pool = build_pool
      pool.ready

      Sync do
        used = nil
        expect do
          pool.with_connection do |connection|
            used = connection
            raise Mongo::Error::OperationFailure, "duplicate key"
          end
        end.to raise_error(Mongo::Error::OperationFailure)

        expect(used.closed?).to be(false)
        expect(pool.available_count).to eq(1)
      end
    end
  end

  describe "#clear" do
    it "bumps the generation and retires stale connections" do
      pool = build_pool
      pool.ready

      Sync do
        old = pool.check_out
        pool.check_in(old)

        pool.clear
        expect(pool.ready?).to be(true)

        fresh = pool.check_out
        expect(fresh).not_to equal(old)
        expect(old.closed?).to be(true)
        pool.check_in(fresh)
      end
    end

    it "pauses the pool and wakes waiters when the server is unknown" do
      pool = build_pool(max_size: 1, wait_timeout: 5)
      pool.ready

      Sync do
        held = pool.check_out
        error = nil

        waiter = Async do
          pool.check_out
        rescue Mongo::Error => e
          error = e
        end

        sleep 0.01
        started = monotonic_time

        # The driver's SDAM monitor thread clears the pool.
        Thread.new do
          fake_server.unknown!
          pool.clear
        end.join

        waiter.wait
        expect(error).to be_a(Mongo::Error::PoolClearedError)
        expect(monotonic_time - started).to be < 1
        expect(pool.paused?).to be(true)

        pool.check_in(held)
        expect(held.closed?).to be(true)
      end
    end

    it "interrupts in-use connections when asked to" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out

        pool.clear(interrupt_in_use_connections: true)
        sleep 0.001

        expect(connection.interrupted?).to be(true)
        expect(connection.closed?).to be(true)
        pool.check_in(connection)
        expect(pool.size).to eq(0)
      end
    end

    it "leaves in-use connections alone otherwise" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out
        pool.clear
        sleep 0.001

        expect(connection.closed?).to be(false)
        pool.check_in(connection)
      end
    end
  end

  describe "#ready" do
    it "allows checkouts again after being paused" do
      pool = build_pool
      pool.ready

      Sync do
        fake_server.unknown!
        pool.clear
        expect { pool.check_out }.to raise_error(Mongo::Error::PoolClearedError)

        fake_server.recover!
        pool.ready
        pool.check_in(pool.check_out)
      end
    end
  end

  describe "#close" do
    it "retires idle connections, and in-use ones when they're checked in" do
      pool = build_pool
      pool.ready

      Sync do
        idle = pool.check_out
        in_use = pool.check_out
        pool.check_in(idle)

        pool.close
        expect(pool.closed?).to be(true)
        expect(idle.disconnect_reasons).to eq([:pool_closed])

        pool.check_in(in_use)
        expect(in_use.disconnect_reasons).to eq([:pool_closed])
        expect(pool.stats).to include(idle: 0, in_use: 0)
        expect { pool.size }.to raise_error(Mongo::Error::PoolClosedError)
      end
    end

    it "disconnects in-use connections with force" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out
        pool.close(force: true)
        expect(connection.closed?).to be(true)
      end
    end

    it "stays usable with stay_ready" do
      pool = build_pool
      pool.ready

      Sync { pool.check_in(pool.check_out) }
      pool.close(stay_ready: true)

      expect(pool.ready?).to be(true)
      expect(pool.size).to eq(0)
    end

    it "retires idle connections from the maintenance fiber when closed from another thread" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out
        pool.check_in(connection)

        Thread.new { pool.close }.join
        sleep 0.001

        expect(connection.disconnect_reasons).to eq([:pool_closed])
      end
    end

    it "is idempotent" do
      pool = build_pool
      pool.close
      expect(pool.close).to be(true)
      expect(pool.disconnect!).to be(true)
    end
  end

  describe "maintenance" do
    it "populates to min_size after the first checkout" do
      pool = build_pool(min_size: 3, max_size: 5)
      pool.ready

      Sync do
        connection = pool.check_out
        sleep 0.001

        expect(pool.size).to eq(3)
        expect(pool.populator.running?).to be(true)
        pool.check_in(connection)
      end
    end

    it "does not start threads when the pool is re-readied from another thread" do
      pool = build_pool(min_size: 2, max_size: 5)
      pool.ready

      Sync do
        pool.check_in(pool.check_out)
        threads = Thread.list.size

        Thread.new do
          fake_server.unknown!
          pool.clear
          fake_server.recover!
          pool.ready
        end.join

        expect(Thread.list.size).to eq(threads)
        sleep 0.001
        expect(pool.size).to eq(2)
      end
    end

    it "retires connections idle for longer than max_idle_time" do
      pool = build_pool(max_idle_time: 0.01)
      pool.ready

      Sync do
        pool.check_in(pool.check_out)
        sleep 0.02

        # Called periodically by the driver's socket reaper thread.
        pool.close_idle_sockets
        sleep 0.001

        expect(pool.size).to eq(0)
      end
    end

    it "is restarted on the next reactor" do
      pool = build_pool
      pool.ready

      Sync { pool.check_in(pool.check_out) }
      expect(pool.populator.running?).to be(false)

      Sync do
        pool.check_in(pool.check_out)
        expect(pool.populator.running?).to be(true)
      end
    end

    it "does not reuse connections created in another process" do
      pool = build_pool
      pool.ready

      Sync do
        connection = pool.check_out
        pool.check_in(connection)
        allow(connection).to receive(:pid).and_return(Process.pid + 1)

        expect(pool.check_out).not_to equal(connection)
        expect(connection.closed?).to be(true)
      end
    end
  end

  describe "fallback pool" do
    it "is used for checkouts outside an Async task, sharing the generation manager" do
      pool = build_pool
      pool.ready

      fallback = instance_double(Mongo::Server::ConnectionPool, check_out: :stock_connection, ready: nil)
      allow(Mongo::Server::ConnectionPool).to receive(:new).and_return(fallback)
      allow(fallback).to receive(:instance_variable_set)

      expect(pool.check_out).to eq(:stock_connection)
      expect(Mongo::Server::ConnectionPool).to have_received(:new).with(fake_server, hash_including(min_size: 0))
      expect(fallback).to have_received(:instance_variable_set).with(:@generation_manager, pool.generation_manager)
    end

    it "is used for Async tasks on threads other than the owner" do
      pool = build_pool
      pool.ready

      fallback = instance_double(Mongo::Server::ConnectionPool, check_out: :stock_connection, ready: nil, instance_variable_set: nil)
      allow(Mongo::Server::ConnectionPool).to receive(:new).and_return(fallback)

      Sync do
        connection = pool.check_out
        expect(connection).to be_a(Async::Mongo::Connection)

        other = Thread.new { Sync { pool.check_out } }.value
        expect(other).to eq(:stock_connection)

        pool.check_in(connection)
      end
    end

    it "is used inside Fiber.blocking" do
      pool = build_pool
      pool.ready

      fallback = instance_double(Mongo::Server::ConnectionPool, check_out: :stock_connection, ready: nil, instance_variable_set: nil)
      allow(Mongo::Server::ConnectionPool).to receive(:new).and_return(fallback)

      Sync do
        expect(Fiber.blocking { pool.check_out }).to eq(:stock_connection)
      end
    end

    it "receives lifecycle calls once it exists" do
      pool = build_pool
      pool.ready

      fallback = instance_double(Mongo::Server::ConnectionPool, check_out: :stock_connection, ready: nil, instance_variable_set: nil, clear: true, close: true)
      allow(Mongo::Server::ConnectionPool).to receive(:new).and_return(fallback)
      pool.check_out

      pool.clear(service_id: nil)
      pool.close

      expect(fallback).to have_received(:clear).with({ service_id: nil })
      expect(fallback).to have_received(:close)
    end
  end
end
