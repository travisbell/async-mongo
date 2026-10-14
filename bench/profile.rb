# frozen_string_literal: true

# Profiles the driver's hot path in-process (no HTTP): concurrent find_ones from
# Async tasks, with or without async-mongo. Prints the top frames by CPU time.
#
#   bundle exec ruby bench/profile.rb [--mode both|pool|socket|none] [--fibers 64] [--seconds 5]

require "optparse"
require "stackprof"
require "mongo"
require "async"
require "async/barrier"

options = { mode: "both", fibers: 64, seconds: 5.0, limit: 40, out: nil }
OptionParser.new do |parser|
  parser.on("--mode MODE", ["both", "pool", "socket", "none"]) { |v| options[:mode] = v }
  parser.on("--fibers N", Integer) { |v| options[:fibers] = v }
  parser.on("--seconds N", Float) { |v| options[:seconds] = v }
  parser.on("--limit N", Integer) { |v| options[:limit] = v }
  parser.on("--out PATH", "Also write the raw profile (for stackprof --method etc.)") { |v| options[:out] = v }
end.parse!

unless options[:mode] == "none"
  $LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
  require "async/mongo"
  Async::Mongo.install!(pool: options[:mode] != "socket", socket: options[:mode] != "pool")
end

Mongo::Logger.logger.level = Logger::WARN
client = Mongo::Client.new(ENV.fetch("MONGODB_URI", "mongodb://localhost:27017/async_mongo_bench"), max_pool_size: Integer(ENV.fetch("POOL", "16")))
documents = client[:documents]

run = lambda do |seconds|
  count = 0
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds

  Sync do
    barrier = Async::Barrier.new
    options[:fibers].times do
      barrier.async do
        while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
          documents.find(_id: rand(10_000)).projection(body: 0).first
          count += 1
        end
      end
    end
    barrier.wait
  end

  count
end

run.call(1) # Warm up (connections, YJIT).

count = nil
profile = StackProf.run(mode: :cpu, interval: 200, raw: true) { count = run.call(options[:seconds]) }
File.binwrite(options[:out], Marshal.dump(profile)) if options[:out]

puts "#{options[:mode]}: #{(count / options[:seconds]).round} queries/s with #{options[:fibers]} fibers"
StackProf::Report.new(profile).print_text(false, options[:limit])
