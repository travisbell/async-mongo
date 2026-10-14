# frozen_string_literal: true

# End-to-end failure handling through the driver (SDAM + retries) with the
# async pool, using fail points. Needs a MongoDB server that allows test
# commands (mongod --setParameter enableTestCommands=1); skipped otherwise.
RSpec.describe("Failure handling with async-mongo installed") do
  uri = ENV.fetch("MONGODB_URI", "mongodb://localhost:27017/async_mongo_test")

  test_commands = begin
    probe = Mongo::Client.new(uri, server_selection_timeout: 1, connect_timeout: 1)
    probe.use(:admin).database.command(configureFailPoint: "failCommand", mode: "off")
    true
  rescue Mongo::Error
    false
  ensure
    probe&.close
  end

  before(:all) { skip("needs a MongoDB with test commands enabled at #{uri}") unless test_commands }

  let(:events) { [] }
  let(:subscriber) do
    recorded = events
    Class.new { define_method(:published) { |event| recorded << event } }.new
  end

  let(:client) do
    Mongo::Client.new(uri, max_pool_size: 4, server_selection_timeout: 5, heartbeat_frequency: 60).tap do |client|
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
    end
  end

  let(:admin) { client.use(:admin).database }
  let(:collection) { client[:"async_mongo_failure_#{SecureRandom.hex(4)}"] }

  after do
    admin.command(configureFailPoint: "failCommand", mode: "off") if test_commands
    collection.drop if test_commands
    client.close
  end

  def fail_next(command, **data)
    admin.command(configureFailPoint: "failCommand", mode: { times: 1 }, data: { failCommands: [command] }.merge(data))
  end

  def pool
    client.cluster.next_primary.pool
  end

  it "clears the pool on a network error and retries the read" do
    Sync do
      collection.insert_one(a: 1)
      expect(pool).to be_a(Async::Mongo::ConnectionPool)

      fail_next("find", closeConnection: true)
      expect(collection.find(a: 1).first["a"]).to eq(1)

      expect(events.grep(Mongo::Monitoring::Event::Cmap::PoolCleared).size).to eq(1)
      expect(events.grep(Mongo::Monitoring::Event::Cmap::ConnectionClosed).map(&:reason)).to include(:error)
    end
  end

  it "clears the pool on a state change error" do
    Sync do
      collection.insert_one(a: 1)

      fail_next("insert", errorCode: 91) # ShutdownInProgress
      begin
        collection.insert_one(a: 2)
      rescue Mongo::Error::OperationFailure
        # Retried on replica sets (retryable writes); raised on a standalone server.
      end

      expect(events.grep(Mongo::Monitoring::Event::Cmap::PoolCleared).size).to eq(1)
      collection.insert_one(a: 3)
      expect(collection.find(a: 3).count).to eq(1)
    end
  end

  it "keeps the pool on an ordinary command error" do
    Sync do
      collection.insert_one(a: 1)

      fail_next("find", errorCode: 2) # BadValue: not retryable, no SDAM change
      expect { collection.find(a: 1).first }.to raise_error(Mongo::Error::OperationFailure)

      expect(events.grep(Mongo::Monitoring::Event::Cmap::PoolCleared)).to be_empty
      expect(collection.find(a: 1).first["a"]).to eq(1)
    end
  end

  it "times out checkouts while connections are blocked, then recovers" do
    client = Mongo::Client.new(uri, max_pool_size: 1, wait_queue_timeout: 0.2, server_selection_timeout: 5)
    documents = client[collection.name]

    Sync do
      documents.insert_one(a: 1)
      admin.command(configureFailPoint: "failCommand", mode: { times: 1 }, data: { failCommands: ["find"], blockConnection: true, blockTimeMS: 1000 })

      slow = Async { documents.find(a: 1).first }
      sleep(0.1)

      expect { documents.find(a: 1).first }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout)
      expect(slow.wait["a"]).to eq(1)
      expect(documents.find(a: 1).first["a"]).to eq(1)
    end
  ensure
    client.close
  end
end
