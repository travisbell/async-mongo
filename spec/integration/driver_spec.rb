# frozen_string_literal: true

# End-to-end tests against a real MongoDB. Set MONGODB_URI to point elsewhere;
# they're skipped when no server is reachable.
RSpec.describe("MongoDB driver with async-mongo installed") do
  uri = ENV.fetch("MONGODB_URI", "mongodb://localhost:27017/async_mongo_test")

  available = begin
    probe = Mongo::Client.new(uri, server_selection_timeout: 1, connect_timeout: 1)
    probe.database.command(ping: 1)
    true
  rescue Mongo::Error
    false
  ensure
    probe&.close
  end

  before(:all) { skip("no MongoDB at #{uri}") unless available }

  let(:options) { {} }
  let(:client) { Mongo::Client.new(uri, { max_pool_size: 10, server_selection_timeout: 5 }.merge(options)) }
  let(:collection) { client[:"async_mongo_#{SecureRandom.hex(4)}"] }

  after do
    collection.drop if available
    client.close
  end

  def server
    client.cluster.next_primary
  end

  def pool
    server.pool
  end

  def count_threads(&block)
    threads = 0
    TracePoint.new(:thread_begin) { threads += 1 }.enable(&block)
    threads
  end

  it "uses the async pool for servers" do
    Sync { collection.insert_one(a: 1) }

    expect(pool).to be_a(Async::Mongo::ConnectionPool)
    expect(pool.stats[:fallback]).to be_nil
  end

  it "handles many concurrent operations within max_pool_size, without spawning threads" do
    Sync { collection.insert_many(100.times.map { |i| { i: i } }) }
    results = nil
    peak = 0

    threads = count_threads do
      Sync do
        barrier = Async::Barrier.new
        tasks = 200.times.map do |i|
          barrier.async do
            peak = [peak, pool.size].max
            collection.find(i: i % 100).first["i"]
          end
        end
        barrier.wait
        results = tasks.map(&:wait)
      end
    end

    expect(results).to eq(200.times.map { |i| i % 100 })
    expect(peak).to be <= 10
    expect(threads).to eq(0) if Async::Mongo.socket_installed?
  end

  it "falls back to a stock pool for plain threads, sharing generations" do
    Sync { collection.insert_one(a: 1) }

    values = 4.times.map { Thread.new { collection.find(a: 1).first["a"] } }.map(&:value)
    expect(values).to eq([1, 1, 1, 1])

    fallback = pool.instance_variable_get(:@fallback_pool)
    expect(fallback).to be_a(Mongo::Server::ConnectionPool)
    expect(fallback.generation_manager).to equal(pool.generation_manager)

    pool.clear
    expect(fallback.generation).to eq(pool.generation)
  end

  it "discards connections whose fibers were cancelled mid-query" do
    Sync do
      collection.insert_one(a: 1)

      tasks = 3.times.map { Async { collection.find("$where" => "sleep(5000) || true").to_a } }
      sleep 0.2
      expect(pool.stats[:in_use]).to eq(3)

      tasks.each(&:stop)
      tasks.each(&:wait)

      expect(pool.stats[:in_use]).to eq(0)
      results = 10.times.map { Async { collection.find(a: 1).first["a"] } }.map(&:wait)
      expect(results).to all(eq(1))
    end
  end

  context "without read retries" do
    # max_read_retries is deprecated (and logs so), but it is the only way to turn off legacy read retries.
    let(:options) { { retry_reads: false, max_read_retries: 0, logger: Logger.new(IO::NULL) } }

    it "wakes a read blocked on the server when the pool is cleared with interrupt_in_use_connections", if: Async::Mongo::Compatibility.interruptible_io? do
      Sync do
        collection.insert_one(a: 1)
        error = nil

        reader = Async do
          collection.find("$where" => "sleep(10000) || true").to_a
        rescue Mongo::Error => e
          error = e
        end

        sleep 0.2
        started = monotonic_time
        Thread.new { pool.clear(interrupt_in_use_connections: true) }.join
        reader.wait

        expect(error).to be_a(Mongo::Error::PoolClearedError)
        expect(monotonic_time - started).to be < 2
      end
    end
  end

  it "keeps working across reactors" do
    Sync { collection.insert_one(a: 1) }
    Sync { expect(collection.find(a: 1).first["a"]).to eq(1) }
    expect(pool.stats[:maintenance]).to be(false)
  end

  it "maintains min_pool_size" do
    client = Mongo::Client.new(uri, max_pool_size: 5, min_pool_size: 3, server_selection_timeout: 5)

    Sync do
      client.database.command(ping: 1)
      sleep 0.2
      expect(client.cluster.next_primary.pool.stats).to include(idle: 3)
    end
  ensure
    client.close
  end

  it "doesn't reuse connections inherited by a forked child" do
    Sync { collection.insert_one(a: 1) }

    reader, writer = IO.pipe
    pid = fork do
      reader.close
      value = Sync { collection.find(a: 1).first["a"] }
      writer.write(value.to_s)
      exit!(0)
    end
    writer.close
    Process.wait(pid)

    expect(reader.read).to eq("1")
    expect(Sync { collection.find(a: 1).first["a"] }).to eq(1)
  end

  it "closes cleanly" do
    Sync { collection.insert_one(a: 1) }
    current = pool

    client.close
    expect(current.size).to eq(0)

    client.reconnect
    expect(Sync { collection.find(a: 1).first["a"] }).to eq(1)
  end
end
