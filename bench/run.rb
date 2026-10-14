# frozen_string_literal: true

# Benchmarks the same Rack app under Puma (threads, the driver's stock pool)
# and Falcon (fibers), with and without async-mongo, and writes a JSON + HTML
# report. See bench/README.md.
#
#   bundle exec ruby bench/run.rb                       # the default suite (~50 minutes)
#   bundle exec ruby bench/run.rb --quick               # short runs
#   bundle exec ruby bench/run.rb --suites concurrency,connections,attribution,ceiling
#   bundle exec ruby bench/run.rb --servers puma-4x5,falcon-2 --scenarios remote --concurrency 64,256

Warning[:experimental] = false

require "etc"
require "json"
require "mongo"
require "net/http"
require "optparse"
require "rbconfig"
require "socket"
require "time"

require_relative "app"
require_relative "report"
require_relative "../lib/async/mongo/version"

module Bench
  # A server configuration, parsed from names like "puma-4x16" (4 workers × 16
  # threads, stock pool) or "falcon-2" (2 workers, async-mongo). A "-poolN"
  # suffix sets the connection pool size per process (e.g. "falcon-1-pool16").
  class Server
    FAMILIES = {
      "puma" => { label: "Puma + stock mongo", async_mongo: "0" },
      "puma-async-mongo" => { label: "Puma + async-mongo installed", async_mongo: "1" },
      "falcon" => { label: "Falcon + async-mongo", async_mongo: "1" },
      "falcon-stock" => { label: "Falcon + stock mongo", async_mongo: "0" },
      "falcon-pool-only" => { label: "Falcon + async-mongo pool only", async_mongo: "pool" },
      "falcon-socket-only" => { label: "Falcon + async-mongo socket only", async_mongo: "socket" }
    }.freeze

    # Connections per Falcon process unless --pool-size says otherwise. Puma
    # gets one per thread.
    FALCON_POOL_SIZE = 64

    PATTERN = /\A(?<family>puma(?:-async-mongo)?)-(?<workers>\d+)x(?<threads>\d+)(?:-pool(?<pool>\d+))?\z|\A(?<family>falcon(?:-stock|-pool-only|-socket-only)?)-(?<workers>\d+)(?:-pool(?<pool>\d+))?\z/

    class << self
      def parse(name, pool_size:)
        match = PATTERN.match(name) or raise ArgumentError, "Unknown server #{name.inspect} (e.g. puma-1x16, puma-async-mongo-4x5, falcon-1, falcon-1-pool16, falcon-stock-2, falcon-pool-only-1, falcon-socket-only-1)"
        pool_size = Integer(match[:pool]) if match[:pool]
        new(name, match[:family], Integer(match[:workers]), match[:threads] && Integer(match[:threads]), pool_size)
      end
    end

    attr_reader :name, :family, :workers, :threads

    def initialize(name, family, workers, threads, pool_size)
      @name = name
      @family = family
      @workers = workers
      @threads = threads
      @pool_size = pool_size
    end

    def puma?
      @family.start_with?("puma")
    end

    # Puma gets one pooled connection per thread unless --pool-size says otherwise.
    def pool_size
      @pool_size || (puma? ? @threads : FALCON_POOL_SIZE)
    end

    # E.g. "Puma (workers: 4, threads: 16, connection pool: 16 per worker)".
    def label
      details = ["workers: #{@workers}"]
      details << "threads: #{@threads}" if puma?
      details << "connection pool: #{pool_size}#{" per worker" if @workers > 1}"
      "#{FAMILIES.fetch(@family)[:label]} (#{details.join(", ")})"
    end

    def async_mongo
      FAMILIES.fetch(@family)[:async_mongo]
    end

    def command(port)
      if puma?
        # One worker means Puma's single mode (no cluster master process).
        workers = @workers > 1 ? ["-w", @workers.to_s] : []
        ["bundle", "exec", "puma", "bench/config.ru", "-b", "tcp://127.0.0.1:#{port}", "-t", "#{@threads}:#{@threads}", *workers, "-e", "production", "-s"]
      else
        ["bundle", "exec", "falcon", "serve", "--bind", "http://127.0.0.1:#{port}", "--count", @workers.to_s, "--config", "bench/config.ru", "--no-restart"]
      end
    end

    def to_h
      { name: @name, label: label, family: @family, workers: @workers, threads: @threads, pool_size: pool_size, connections: pool_size * @workers }
    end
  end

  class Runner
    ROOT = File.expand_path("..", __dir__)

    SCENARIOS = {
      "ping" => { path: "/ping", description: "no database: the HTTP server and load generator ceiling" },
      "find" => { path: "/find", description: "one indexed find_one per request" },
      "fanout" => { path: "/fanout", description: "#{FANOUT} find_ones per request (concurrent under Falcon, sequential under Puma)" },
      "remote" => { path: "/find?remote=1", description: "one find_one per request, with LATENCY added to each database round trip" },
      "remote_fanout" => { path: "/fanout?remote=1", description: "#{FANOUT} find_ones per request (concurrent under Falcon, sequential under Puma), with LATENCY added to each database round trip" }
    }.freeze

    SUITES = {
      "concurrency" => {
        title: "Fibers vs threads",
        description: "Puma's thread pool against Falcon's fibers, with the stock MongoDB driver and with async-mongo, as the number of concurrent clients grows. " \
          "Puma runs with 4, 8 or 16 threads and a connection per thread, as Ruby apps usually do; its concurrency is its thread count. " \
          "Falcon runs a fiber per request and has 64 connections per process; its concurrency is its connection pool.",
        servers: [
          "puma-1x4", "puma-1x8", "puma-1x16", "falcon-stock-1", "falcon-1",
          "puma-4x4", "puma-4x8", "puma-4x16", "falcon-stock-4", "falcon-4"
        ],
        scenarios: ["find", "remote", "remote_fanout"],
        concurrency: [1, 4, 16, 64, 256, 1024]
      },
      "connections" => {
        title: "Same number of connections",
        description: "Puma with 4, 8, 16 and 64 threads (and a connection per thread) against Falcon + async-mongo with the same number of connections per process, " \
          "so both have the same database concurrency and the difference is fibers and the driver. More connections cost Puma a thread each; they cost Falcon nothing else.",
        servers: [
          "puma-1x4", "falcon-1-pool4", "puma-1x8", "falcon-1-pool8", "puma-1x16", "falcon-1-pool16", "puma-1x64", "falcon-1",
          "puma-4x4", "falcon-4-pool4", "puma-4x8", "falcon-4-pool8", "puma-4x16", "falcon-4-pool16", "puma-4x64", "falcon-4"
        ],
        scenarios: ["find", "remote", "remote_fanout"],
        concurrency: [64, 256]
      },
      "attribution" => {
        title: "Where the gains come from",
        description: "One Falcon process with neither, either or both parts of async-mongo installed.",
        servers: ["falcon-stock-1", "falcon-socket-only-1", "falcon-pool-only-1", "falcon-1"],
        scenarios: ["find", "remote_fanout"],
        concurrency: [256]
      },
      "ceiling" => {
        title: "Without a database",
        description: "The HTTP servers alone, as a ceiling for the other results.",
        servers: ["puma-1x16", "falcon-1", "puma-4x16", "falcon-4"],
        scenarios: ["ping"],
        concurrency: [64, 256]
      }
    }.freeze

    DEFAULTS = {
      suites: ["concurrency"],
      duration: 10.0,
      warmup: 3.0,
      pool_size: nil,
      latency_ms: 5.0,
      load_processes: 4,
      yjit: true,
      uri: ENV.fetch("MONGODB_URI", "mongodb://localhost:27017/async_mongo_bench")
    }.freeze

    def initialize(options, argv: [])
      @options = DEFAULTS.merge(options)
      @argv = argv
      @suites = build_suites
      @results = []
    end

    def run
      raise_file_limit
      seed
      @environment = environment
      puts header

      with_latency_proxy do
        plan.each { |server, measurements| benchmark(server, measurements) }
      end

      data = { environment: @environment, suites: @suites, results: @results }
      json_path, html_path = save(data)

      puts
      puts "Results: #{json_path}"
      puts "Report:  #{html_path}"
    end

    private

    def build_suites
      if @options[:servers] || @options[:scenarios] || @options[:concurrency]
        custom = {
          title: "Custom run",
          description: "Servers, scenarios and concurrency levels chosen on the command line.",
          servers: @options[:servers] || SUITES["concurrency"][:servers],
          scenarios: @options[:scenarios] || SUITES["concurrency"][:scenarios],
          concurrency: @options[:concurrency] || SUITES["concurrency"][:concurrency]
        }
        return { "custom" => custom }
      end

      @options[:suites].to_h { |name| [name, SUITES.fetch(name)] }
    end

    # Each server runs once, measuring everything any suite needs from it.
    def plan
      measurements = Hash.new { |hash, key| hash[key] = [] }

      @suites.each_value do |suite|
        suite[:servers].each do |name|
          suite[:scenarios].each do |scenario|
            suite[:concurrency].each { |concurrency| measurements[name] |= [[scenario, concurrency]] }
          end
        end
      end

      measurements.map { |name, list| [Server.parse(name, pool_size: @options[:pool_size]), list] }
    end

    # Up to 1024 clients plus the servers' database connections need more open
    # files than some systems allow by default (e.g. 256 on macOS).
    def raise_file_limit
      soft, hard = Process.getrlimit(:NOFILE)
      wanted = [16_384, hard].min
      Process.setrlimit(:NOFILE, wanted, hard) if soft < wanted
    rescue Errno::EPERM, Errno::EINVAL
      warn("Couldn't raise the open file limit (#{soft}); runs with many clients may fail.")
    end

    def mongo_client
      @mongo_client ||= Mongo::Client.new(@options[:uri], server_selection_timeout: 5)
    end

    def seed
      documents = mongo_client[:documents]
      return if documents.estimated_document_count == DOCUMENTS

      puts "Seeding #{DOCUMENTS} documents into #{documents.namespace}..."
      documents.drop
      DOCUMENTS.times.each_slice(1000) do |ids|
        documents.insert_many(ids.map { |id| { _id: id, title: "Document #{id}", rank: rand(1000), tags: ["a", "b", "c"].sample(2), body: "x" * 512 } })
      end
    end

    def benchmark(server, measurements)
      port = free_port

      puts
      puts "== #{server.label}"

      with_server(server, port) do |pgid|
        measurements.group_by(&:first).each do |scenario, list|
          url = "http://127.0.0.1:#{port}#{SCENARIOS.fetch(scenario)[:path]}"
          concurrencies = list.map(&:last).sort

          load_test(url, concurrencies.max, @options[:warmup]) if @options[:warmup] > 0

          concurrencies.each do |concurrency|
            cpu_before = cpu_seconds(pgid)
            result = load_test(url, concurrency, @options[:duration])
            cpu = cpu_seconds(pgid) - cpu_before
            stats = server_stats(port)

            row = server.to_h.merge(
              server: server.name,
              scenario: scenario,
              concurrency: concurrency,
              rps: result["rps"],
              latency_ms: result["latency_ms"],
              requests: result["requests"],
              errors: result["errors"],
              error_samples: result["error_samples"],
              connect_errors: result["connect_errors"],
              cpu_percent: (cpu / result["duration"] * 100).round(1),
              rss_mb: rss_mb(pgid),
              threads_per_process: stats["threads"],
              pools: stats["pools"]
            )
            @results << row

            puts format(
              "  %-13s c=%-4d %9.1f req/s  p50 %7.2fms  p99 %8.2fms  errors %-5d cpu %6.1f%%  rss %5dMB",
              scenario, concurrency, row[:rps], row[:latency_ms]["p50"] || 0, row[:latency_ms]["p99"] || 0,
              row[:errors], row[:cpu_percent], row[:rss_mb]
            )
            puts "    error samples: #{row[:error_samples].join("; ")}" unless row[:error_samples].empty?
            puts "    #{row[:connect_errors]} clients couldn't connect before measuring" if row[:connect_errors].to_i.positive?
          end
        end
      end
    end

    def with_server(server, port)
      env = {
        "ASYNC_MONGO" => server.async_mongo,
        "SERVER_MODEL" => server.name,
        "MONGODB_URI" => @options[:uri],
        "MONGODB_REMOTE_URI" => @remote_uri.to_s,
        "MONGO_POOL_SIZE" => server.pool_size.to_s,
        "RUBY_YJIT_ENABLE" => @options[:yjit] ? "1" : "0",
        "CONSOLE_LEVEL" => "warn",
        "RACK_ENV" => "production"
      }

      log = File.join(ROOT, "tmp", "#{server.name}.log")
      Dir.mkdir(File.dirname(log)) unless Dir.exist?(File.dirname(log))

      pid = Process.spawn(env, *server.command(port), chdir: ROOT, pgroup: true, out: log, err: log)
      wait_until_up(port, pid, log)
      yield pid
    ensure
      stop(pid) if pid
    end

    # Starts bench/latency_proxy.rb in front of the database, for the remote scenarios.
    def with_latency_proxy
      return yield unless @suites.each_value.any? { |suite| suite[:scenarios].any? { |scenario| scenario.start_with?("remote") } }

      address = mongo_client.cluster.servers.first.address
      port = free_port
      pid = Process.spawn({ "RUBY_YJIT_ENABLE" => "1" }, RbConfig.ruby, File.join(__dir__, "latency_proxy.rb"), port.to_s, "#{address.host}:#{address.port}", @options[:latency_ms].to_s, out: File::NULL)

      # directConnection, so the driver doesn't discover (and connect directly to) replica set members.
      @remote_uri = "mongodb://127.0.0.1:#{port}/#{mongo_client.database.name}?directConnection=true"
      wait_for_port(port)
      yield
    ensure
      if pid
        Process.kill("TERM", pid)
        Process.wait(pid)
      end
    end

    def wait_for_port(port)
      deadline = monotonic_time + 10

      begin
        TCPSocket.new("127.0.0.1", port).close
      rescue SystemCallError
        raise "Latency proxy didn't start" if monotonic_time > deadline

        sleep(0.05)
        retry
      end
    end

    def wait_until_up(port, pid, log)
      deadline = monotonic_time + 30

      loop do
        raise "Server exited early, see #{log}:\n#{File.read(log).lines.last(20).join}" if Process.wait(pid, Process::WNOHANG)

        begin
          return if Net::HTTP.get_response(URI("http://127.0.0.1:#{port}/ping")).code == "200"
        rescue SystemCallError, IOError, Net::ReadTimeout
          # Not up yet.
        end

        raise "Server didn't start within 30s, see #{log}" if monotonic_time > deadline

        sleep(0.1)
      end
    end

    def stop(pid)
      Process.kill("TERM", -pid)
      deadline = monotonic_time + 10

      until Process.wait(pid, Process::WNOHANG)
        if monotonic_time > deadline
          Process.kill("KILL", -pid)
          Process.wait(pid)
          break
        end

        sleep(0.05)
      end

      # Worker processes can briefly outlive their parent.
      begin
        Process.kill("KILL", -pid)
      rescue Errno::ESRCH
        # All gone.
      end
    rescue Errno::ESRCH, Errno::ECHILD
      # Already gone.
    end

    def load_test(url, concurrency, duration)
      command = [RbConfig.ruby, File.join(__dir__, "load.rb"), url, "--concurrency", concurrency.to_s, "--duration", duration.to_s, "--processes", @options[:load_processes].to_s]
      output = IO.popen({ "RUBY_YJIT_ENABLE" => "1" }, command, &:read)
      raise "Load generator failed for #{url}" unless $?.success?

      JSON.parse(output.lines.last)
    end

    def server_stats(port)
      JSON.parse(Net::HTTP.get(URI("http://127.0.0.1:#{port}/stats")))
    rescue StandardError
      {}
    end

    def group_pids(pgid)
      `pgrep -g #{pgid}`.split.map(&:to_i)
    end

    # Total CPU time (user + system) of the server's process group.
    def cpu_seconds(pgid)
      pids = group_pids(pgid)
      return 0.0 if pids.empty?

      `ps -o time= -p #{pids.join(",")}`.split.sum do |time|
        time.split(":").map(&:to_f).reverse.each_with_index.sum { |value, index| value * (60**index) }
      end
    end

    def rss_mb(pgid)
      pids = group_pids(pgid)
      return 0 if pids.empty?

      `ps -o rss= -p #{pids.join(",")}`.split.sum(&:to_i) / 1024
    end

    def free_port
      server = TCPServer.new("127.0.0.1", 0)
      server.addr[1]
    ensure
      server&.close
    end

    def environment
      {
        time: Time.now.iso8601,
        command: ["bundle exec ruby bench/run.rb", *@argv].join(" "),
        ruby: RUBY_DESCRIPTION,
        yjit: @options[:yjit],
        cpu: `sysctl -n machdep.cpu.brand_string 2>/dev/null`.strip.then { |cpu| cpu.empty? ? `uname -m`.strip : cpu },
        cpus: Etc.nprocessors,
        os: `uname -sr`.strip,
        mongodb_server: mongo_client.database.command(buildInfo: 1).first["version"],
        mongo_driver: Mongo::VERSION,
        async_mongo: Async::Mongo::VERSION,
        puma: gem_version("puma"),
        falcon: gem_version("falcon"),
        duration: @options[:duration],
        warmup: @options[:warmup],
        latency_ms: @options[:latency_ms],
        load_processes: @options[:load_processes],
        scenarios: SCENARIOS.transform_values { |scenario| scenario[:description].sub("LATENCY", "#{format("%g", @options[:latency_ms])}ms") },
        routes: SCENARIOS.transform_values { |scenario| scenario[:path] }
      }
    end

    def gem_version(name)
      Gem.loaded_specs[name]&.version&.to_s
    end

    def header
      env = @environment
      [
        "async-mongo benchmark — #{env[:time]}",
        "#{env[:ruby]} (YJIT #{env[:yjit] ? "on" : "off"}), #{env[:cpu]} ×#{env[:cpus]}",
        "MongoDB #{env[:mongodb_server]}, mongo #{env[:mongo_driver]}, async-mongo #{env[:async_mongo]}, puma #{env[:puma]}, falcon #{env[:falcon]}",
        "#{@options[:duration]}s per run (#{@options[:warmup]}s warmup), remote latency #{format("%g", @options[:latency_ms])}ms, suites: #{@suites.keys.join(", ")}"
      ].join("\n")
    end

    def save(data)
      directory = File.join(__dir__, "results")
      Dir.mkdir(directory) unless Dir.exist?(directory)
      base = File.join(directory, Time.now.strftime("%Y%m%d-%H%M%S"))

      File.write("#{base}.json", JSON.pretty_generate(data))
      File.write("#{base}.html", Report.new(JSON.parse(JSON.generate(data))).to_html)

      ["#{base}.json", "#{base}.html"]
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end

if $PROGRAM_NAME == __FILE__
  argv = ARGV.dup
  options = {}
  list = ->(value) { value.split(",").map(&:strip) }

  OptionParser.new do |parser|
    parser.banner = "Usage: bundle exec ruby bench/run.rb [options]"
    parser.on("--suites LIST", "Comma-separated: #{Bench::Runner::SUITES.keys.join(", ")} (default: concurrency)") { |v| options[:suites] = list.(v) }
    parser.on("--servers LIST", "Custom run: e.g. puma-1x16,puma-4x5,falcon-1,falcon-stock-2,falcon-pool-only-1") { |v| options[:servers] = list.(v) }
    parser.on("--scenarios LIST", "Custom run: #{Bench::Runner::SCENARIOS.keys.join(", ")}") { |v| options[:scenarios] = list.(v) }
    parser.on("--concurrency LIST", "Custom run: client concurrency levels, e.g. 1,64,256") { |v| options[:concurrency] = list.(v).map { Integer(it) } }
    parser.on("--duration SECONDS", Float, "Duration of each run (default 10)") { |v| options[:duration] = v }
    parser.on("--warmup SECONDS", Float, "Warmup per server and scenario (default 3)") { |v| options[:warmup] = v }
    parser.on("--pool-size N", Integer, "max_pool_size per process (default: Puma's thread count, #{Bench::Server::FALCON_POOL_SIZE} for Falcon)") { |v| options[:pool_size] = v }
    parser.on("--latency-ms N", Float, "Latency added to each database round trip in the remote scenarios (default 5)") { |v| options[:latency_ms] = v }
    parser.on("--load-processes N", Integer, "Load generator processes (default 4)") { |v| options[:load_processes] = v }
    parser.on("--[no-]yjit", "Run the servers with YJIT (default on)") { |v| options[:yjit] = v }
    parser.on("--quick", "Short runs, for a smoke test") do
      options[:duration] = 3.0
      options[:warmup] = 1.0
    end
    parser.on("--report JSON", "Only regenerate the HTML report from a results file") do |path|
      html = "#{path.delete_suffix(".json")}.html"
      File.write(html, Bench::Report.new(JSON.parse(File.read(path))).to_html)
      puts "Report: #{html}"
      exit
    end
  end.parse!

  unknown = (options[:suites] || []) - Bench::Runner::SUITES.keys
  abort("Unknown suites: #{unknown.join(", ")}") unless unknown.empty?
  unknown = (options[:scenarios] || []) - Bench::Runner::SCENARIOS.keys
  abort("Unknown scenarios: #{unknown.join(", ")}") unless unknown.empty?
  begin
    (options[:servers] || []).each { |name| Bench::Server.parse(name, pool_size: nil) }
  rescue ArgumentError => e
    abort(e.message)
  end

  Bench::Runner.new(options, argv: argv).run
end
