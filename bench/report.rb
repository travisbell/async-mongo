# frozen_string_literal: true

require "cgi"
require "json"

module Bench
  # Renders the results bench/run.rb writes (as parsed JSON) into a
  # self-contained HTML page: headline numbers, a chart and table per scenario,
  # the environment and the raw data. No external assets.
  #
  #   bundle exec ruby bench/run.rb --report bench/results/<timestamp>.json
  class Report
    # Color follows the server family (never its position). The three main
    # families get the first three slots of a categorical palette, which pass
    # the color-vision-deficiency and contrast checks together in light and dark
    # mode; variants (partial installs, Puma with async-mongo installed) are
    # shown in neutral gray and told apart by their labels, rather than adding
    # hues that can't be distinguished.
    FAMILY_SLOTS = {
      "puma" => "1",
      "falcon-stock" => "2",
      "falcon" => "3",
      "falcon-pool-only" => "neutral",
      "puma-async-mongo" => "neutral",
      "falcon-socket-only" => "neutral"
    }.freeze

    FAMILY_NAMES = {
      "puma" => "Puma + stock mongo",
      "falcon-stock" => "Falcon + stock mongo",
      "falcon" => "Falcon + async-mongo",
      "puma-async-mongo" => "Puma + async-mongo installed",
      "falcon-pool-only" => "Falcon + async-mongo pool only",
      "falcon-socket-only" => "Falcon + async-mongo socket only"
    }.freeze

    # Each scenario's route (for results that predate the runner recording them).
    ROUTES = {
      "ping" => "/ping",
      "find" => "/find",
      "fanout" => "/fanout",
      "remote" => "/find?remote=1",
      "remote_fanout" => "/fanout?remote=1"
    }.freeze

    # Scenarios that don't touch the database: the connection pool is irrelevant,
    # so their servers are shown as plain Puma and Falcon.
    DATABASE_FREE = ["ping"].freeze

    PLAIN_NAMES = { "puma" => "Puma", "falcon-stock" => "Falcon" }.freeze

    # Dash patterns for Puma's thread counts (fewest to most), so they aren't told apart by shade alone.
    DASHES = { "puma-1" => "2 4", "puma-2" => "8 4" }.freeze
    KEY_STYLES = { "2 4" => "dotted", "8 4" => "dashed" }.freeze

    # Line charts (by client count) for suites that sweep at least this many concurrency levels.
    SWEEP_POINTS = 3

    def initialize(data)
      @environment = data.fetch("environment")
      @suites = data.fetch("suites")
      @results = data.fetch("results")
      @chart_id = 0
    end

    def to_html
      <<~HTML
        <!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>async-mongo benchmark</title>
        <style>#{STYLE}</style>
        </head>
        <body>
        <main class="viz-root">
          <header class="page-header">
            <div>
              <h1>async-mongo benchmark</h1>
              <p class="lede">Puma serves concurrent requests with a pool of threads; Falcon runs each request in a fiber, so a process can serve many at once without a thread for each. With the stock MongoDB driver, that concurrency doesn't reach the database. These are the same Rack app and queries on Puma with the stock driver, Falcon with the stock driver, and Falcon with async-mongo, as the number of concurrent clients grows.</p>
              <p class="meta">#{h(environment_line)}</p>
            </div>
            <button type="button" class="theme-toggle" aria-label="Toggle dark mode">Theme</button>
          </header>
          #{highlights}
          <nav class="toc" aria-label="Sections">
            #{@suites.map { |name, suite| %(<a href="##{h(name)}">#{h(suite["title"])}</a>) }.join("\n")}
            <a href="#method">Method</a>
            <a href="#data">Raw data</a>
          </nav>
          #{@suites.map { |name, suite| suite_section(name, suite) }.join("\n")}
          #{method_section}
          #{data_section}
          <div class="tooltip" role="status" hidden></div>
        </main>
        <script type="application/json" id="results-data">#{json_for_script}</script>
        <script>#{SCRIPT}</script>
        </body>
        </html>
      HTML
    end

    private

    # -- Sections --

    def environment_line
      env = @environment
      [
        env["time"]&.sub("T", " ")&.sub(/[+-]\d\d:\d\d\z/, ""),
        "#{env["cpu"]} ×#{env["cpus"]}",
        env["ruby"]&.split(" (")&.first&.sub("ruby", "Ruby"),
        ("YJIT" if env["yjit"]),
        "MongoDB #{env["mongodb_server"]}",
        "mongo #{env["mongo_driver"]}",
        "async-mongo #{env["async_mongo"]}",
        "Puma #{env["puma"]}",
        "Falcon #{env["falcon"]}"
      ].compact.join(" · ")
    end

    # Headline numbers from the fibers-vs-threads sweep (or whatever subset of
    # it was run): Falcon + async-mongo against the other two setups.
    def highlights
      tiles = []
      # Heavy but representative load: 256 clients if measured, else the most measured.
      busiest = lambda do |scenario, workers|
        levels = ["falcon", "puma"].map { |family| @results.select { |r| r["family"] == family && r["scenario"] == scenario && r["workers"] == workers }.map { |r| r["concurrency"] } }
        shared = levels.reduce(:&) || []
        shared.include?(256) ? 256 : shared.max
      end

      p99 = ->(r) { r.dig("latency_ms", "p99") }
      p50 = ->(r) { r.dig("latency_ms", "p50") }
      cores = ->(r) { r["cpu_percent"].to_f / 100 }

      # Each compared against the best Puma configuration at that point.
      [["remote", 4], ["find", 1]].each do |scenario, workers|
        clients = busiest.(scenario, workers) or next
        fast = find_row("falcon", workers, scenario, clients)
        slow = best_puma(workers, scenario, clients) { |r| -r["rps"] }
        next unless fast && slow

        stock = find_row("falcon-stock", workers, scenario, clients)
        note = "#{FAMILY_NAMES["falcon"]}, vs #{number(slow["rps"])} req/s on #{puma_name(slow)}, the best Puma configuration"
        note += ", and #{number(stock["rps"])} on #{FAMILY_NAMES["falcon-stock"]}" if stock
        tiles << tile("#{route(scenario)} · #{process_label(workers)} · #{clients} clients", "#{number(fast["rps"])} req/s", change(fast["rps"], slow["rps"], higher_is_better: true), note)
      end

      if (clients = busiest.("find", 1)) && (fast = find_row("falcon", 1, "find", clients)) && (stock = find_row("falcon-stock", 1, "find", clients))
        note = "p99 on #{FAMILY_NAMES["falcon"]}, vs #{ms(p99.(stock))} on #{FAMILY_NAMES["falcon-stock"]}"
        slow = best_puma(1, "find", clients) { |r| p99.(r) }
        note += " and #{ms(p99.(slow))} on #{puma_name(slow)}" if slow
        tiles << tile("#{route("find")} · 1 process · #{clients} clients", ms(p99.(fast)), change(p99.(fast), p99.(stock), higher_is_better: false), note)
      end

      if (clients = busiest.("remote", 4)) && (fast = find_row("falcon", 4, "remote", clients)) && (slow = best_puma(4, "remote", clients) { |r| -r["rps"] })
        tiles << tile("CPU · #{route("remote")} · 4 processes · #{clients} clients", "#{format("%.1f", cores.(fast))} cores", change(cores.(fast), cores.(slow), higher_is_better: false), "#{FAMILY_NAMES["falcon"]} serving #{number(fast["rps"])} req/s, vs #{format("%.1f", cores.(slow))} cores for #{number(slow["rps"])} req/s on #{puma_name(slow)}")
      end

      if (fast = find_row("falcon", 1, "remote_fanout", 1)) && (slow = best_puma(1, "remote_fanout", 1) { |r| p50.(r) })
        tiles << tile("#{route("remote_fanout")} · 1 client", ms(p50.(fast)), change(p50.(fast), p50.(slow), higher_is_better: false), "median for 5 queries #{latency_label} away, concurrent in fibers, vs #{ms(p50.(slow))} one after another on #{puma_name(slow)}")
      end

      return "" if tiles.empty?

      %(<section class="highlights" aria-label="Highlights">#{tiles.join}</section>)
    end

    def tile(label, value, delta, note)
      <<~HTML
        <div class="tile">
          <div class="tile-label">#{h(label)}</div>
          <div class="tile-value">#{h(value)}</div>
          #{delta}
          <div class="tile-note">#{h(note)}</div>
        </div>
      HTML
    end

    def change(value, baseline, higher_is_better:)
      return "" unless value && baseline && baseline.positive?

      ratio = value / baseline.to_f
      percent = ((ratio - 1) * 100).round
      good = higher_is_better ? percent >= 0 : percent <= 0
      text = if !higher_is_better && ratio < 0.5
        "#{(1 / ratio).round(1)}× lower"
      elsif higher_is_better && ratio >= 2
        "#{ratio.round(1)}× higher"
      else
        "#{"+" if percent.positive?}#{percent}%"
      end

      # The arrow shows the direction of the change; the color, whether that's an improvement.
      %(<div class="tile-delta #{good ? "good" : "bad"}"><span aria-hidden="true">#{ratio >= 1 ? "▲" : "▼"}</span> #{h(text)} <span class="sr">#{good ? "(better)" : "(worse)"}</span></div>)
    end

    def suite_section(name, suite)
      scenarios = suite["scenarios"].select { |scenario| suite_rows(suite, scenario).any? }

      <<~HTML
        <section class="suite" id="#{h(name)}">
          <h2>#{h(suite["title"])}</h2>
          <p class="section-lede">#{h(suite["description"])}</p>
          #{scenarios.map { |scenario| scenario_block(suite, scenario) }.join("\n")}
        </section>
      HTML
    end

    # A few concurrency levels: bars per level, one row per metric.
    def scenario_block(suite, scenario)
      return sweep_block(suite, scenario) if suite["concurrency"].size >= SWEEP_POINTS

      rows = suite_rows(suite, scenario)
      metrics = [
        ["Requests per second", ->(r) { r["rps"] }, "req/s"],
        ["p99 latency", ->(r) { r.dig("latency_ms", "p99") }, "ms"]
      ]
      # Throughput per core of CPU used, when servers with different process counts are compared.
      metrics << ["Requests per second per CPU core", ->(r) { per_core(r) }, "req/s"] if rows.map { |r| r["workers"] }.uniq.size > 1

      charts = metrics.flat_map do |title, metric, unit|
        suite["concurrency"].map do |concurrency|
          bar_chart("#{title}, #{concurrency} clients", rows.select { |r| r["concurrency"] == concurrency }, metric, unit: unit)
        end
      end

      scenario_article(scenario, legend(rows), charts, rows)
    end

    # A concurrency sweep: lines by client count, one row per metric and one
    # column per process count, so 1 and 4 processes sit side by side.
    def sweep_block(suite, scenario)
      rows = suite_rows(suite, scenario)
      process_counts = rows.map { |r| r["workers"] }.uniq.sort
      metrics = [
        ["Requests per second", ->(r) { r["rps"] }, :linear, "req/s"],
        ["p99 latency", ->(r) { r.dig("latency_ms", "p99") }, :log, "ms"],
        ["CPU used", ->(r) { r["cpu_percent"].to_f / 100 }, :linear, "cores"]
      ]

      charts = metrics.flat_map do |title, metric, scale, unit|
        process_counts.map do |workers|
          servers = suite["servers"].select { |server| rows.any? { |r| r["server"] == server && r["workers"] == workers } }
          line_chart("#{title} · #{process_label(workers)}", servers, scenario, suite["concurrency"], metric, scale: scale, unit: unit)
        end
      end

      scenario_article(scenario, legend(rows, detailed: false), charts, rows)
    end

    def scenario_article(scenario, legend, charts, rows)
      <<~HTML
        <article class="scenario">
          <h3><code class="route">#{h(route(scenario))}</code></h3>
          <p class="scenario-description">#{h(@environment.dig("scenarios", scenario).to_s)}</p>
          #{legend}
          <div class="charts">#{charts.join}</div>
          #{table(rows)}
        </article>
      HTML
    end

    def method_section
      env = @environment

      <<~HTML
        <section class="suite" id="method">
          <h2>Method</h2>
          <ul class="notes">
            <li>Each server runs on its own, on a free port, with YJIT #{env["yjit"] ? "on" : "off"}. Every run lasts #{h(format("%g", env["duration"]))}s after a #{h(format("%g", env["warmup"]))}s warmup per scenario.</li>
            <li>The load generator is closed-loop: N persistent HTTP/1.1 connections, each sending its next request when the previous response arrives (#{h(env["load_processes"])} processes, on the same machine). Every connection is opened before measuring starts, so the numbers are for serving requests, not for accepting a burst of new connections.</li>
            <li>Puma works on as many requests as it has threads and leaves the rest of the clients waiting in the kernel. Falcon takes every request at once, each in a fiber; with 1,024 clients on one process that's 1,024 fibers for the garbage collector to scan, which is part of why throughput per process falls off at that point.</li>
            <li>Remote scenarios go through a proxy that delays every reply from the database by #{h(latency_label)}, simulating a database farther away than localhost.</li>
            <li>CPU is the server's total (all of its processes; 100% is one core) and memory is the resident size of all of its processes; Falcon runs a supervisor process next to its workers. Threads are per process, including the driver's monitoring threads.</li>
            <li>When queries wait on the network, throughput is capped at about <em>connections ÷ round trip</em> for any server; the Connections column is the total across processes.</li>
            <li>The database ran on the same machine, so at high request rates it, rather than the app server, can be the limit; <code>/ping</code> shows the ceiling without a database.</li>
            <li>“+ stock mongo” is the MongoDB driver as it ships; “+ async-mongo” has async-mongo installed. Servers are labeled with their configuration: worker processes, threads per worker (Puma) and <code>max_pool_size</code> per worker. Puma runs with 4, 8 or 16 threads and a connection per thread, as Ruby apps usually do, so its concurrency is its thread count; Falcon runs a fiber per request with 64 connections per process, so its concurrency is its connection pool. Puma's thread counts are told apart by shade and dash pattern, and the headline numbers compare against the best Puma configuration at each point.</li>
            <li>Line charts space the client counts evenly (each is 4× the last); latency is on a log scale.</li>
            <li>Under Puma, threads limit how many queries run at once; under Falcon, the connection pool does. Four Falcon processes with 64 connections each can keep the local database at its limit (about 13,000–14,000 req/s here), where queries queue inside MongoDB and tail latency rises; fewer connections per process trade a little throughput for less database load and lower tail latency (see the README's “Sizing the connection pool”).</li>
          </ul>
          <dl class="env">
            #{env_rows}
          </dl>
          <p>Reproduce with <code>#{h(env["command"])}</code>; see bench/README.md.</p>
        </section>
      HTML
    end

    def env_rows
      {
        "Date" => @environment["time"],
        "Machine" => "#{@environment["cpu"]} ×#{@environment["cpus"]}, #{@environment["os"]}",
        "Ruby" => @environment["ruby"],
        "MongoDB" => @environment["mongodb_server"],
        "Gems" => "mongo #{@environment["mongo_driver"]}, async-mongo #{@environment["async_mongo"]}, puma #{@environment["puma"]}, falcon #{@environment["falcon"]}"
      }.map { |key, value| "<dt>#{h(key)}</dt><dd>#{h(value)}</dd>" }.join("\n")
    end

    def data_section
      <<~HTML
        <section class="suite" id="data">
          <h2>Raw data</h2>
          <p>#{@results.size} measurements. <button type="button" class="download">Download JSON</button></p>
        </section>
      HTML
    end

    # -- Charts --

    # One line per server across the concurrency levels (evenly spaced: the
    # default levels are powers of 4), with a marker at each measurement and a
    # crosshair tooltip listing every server at the hovered level.
    def line_chart(title, servers, scenario, levels, metric, scale:, unit:)
      width = 600
      height = 270
      left = 58
      right = 18
      top = 30
      bottom = 44
      plot_width = width - left - right
      plot_height = height - top - bottom
      plot_bottom = top + plot_height

      series = servers.map do |server|
        points = levels.map { |level| (r = row(server, scenario, level)) && [r, metric.(r)] }
        { server: server, row: points.compact.first&.first, points: points }
      end
      values = series.flat_map { |line| line[:points].compact.map(&:last) }.select { |v| v&.positive? }

      ticks, y = if scale == :log && values.any?
        log_scale(values.min, values.max, top, plot_height)
      else
        ticks = nice_ticks(values.max || 0)
        [ticks, ->(value) { plot_bottom - (value.to_f / ticks.last * plot_height) }]
      end

      x = ->(index) { levels.size == 1 ? left + (plot_width / 2.0) : left + (index * plot_width.to_f / (levels.size - 1)) }

      svg = +""
      ticks.each do |tick|
        svg << %(<line class="grid" x1="#{left}" x2="#{width - right}" y1="#{f(y.(tick))}" y2="#{f(y.(tick))}"/>) unless tick.zero?
        svg << %(<text class="axis-label" x="#{left - 8}" y="#{f(y.(tick) + 4)}" text-anchor="end">#{h(axis_number(tick))}</text>)
      end
      svg << %(<text class="axis-title" x="0" y="#{top - 16}" text-anchor="start">#{h(scale == :log ? "#{unit}, log scale" : unit)}</text>)
      svg << %(<line class="baseline" x1="#{left}" x2="#{width - right}" y1="#{plot_bottom}" y2="#{plot_bottom}"/>)

      levels.each_with_index do |level, i|
        svg << %(<text class="axis-label" x="#{f(x.(i))}" y="#{plot_bottom + 18}" text-anchor="middle">#{h(number(level))}</text>)
      end
      svg << %(<text class="axis-title" x="#{f(left + (plot_width / 2.0))}" y="#{height - 6}" text-anchor="middle">concurrent clients</text>)

      svg << %(<line class="crosshair" x1="0" x2="0" y1="#{top}" y2="#{plot_bottom}"/>)

      series.each do |line|
        next unless line[:row]

        slot = row_slot(line[:row], shades: true)
        color = "var(--series-#{slot})"
        dash = DASHES[slot] ? "; stroke-dasharray: #{DASHES[slot]}" : ""
        coordinates = line[:points].each_with_index.filter_map { |point, i| [x.(i), y.(point.last)] if point && point.last&.positive? }
        svg << %(<path class="series-line" d="M#{coordinates.map { |px, py| "#{f(px)},#{f(py)}" }.join("L")}" style="stroke: #{color}#{dash}"/>)
        coordinates.each { |px, py| svg << %(<circle class="marker" cx="#{f(px)}" cy="#{f(py)}" r="4" style="fill: #{color}"/>) }
      end

      # One hit column per level, wider than the markers.
      band = levels.size == 1 ? plot_width : plot_width.to_f / (levels.size - 1)
      levels.each_with_index do |level, i|
        rows = series.filter_map do |line|
          point = line[:points][i] or next
          slot = row_slot(point.first, shades: true)
          [slot, server_label(point.first), format_metric(point.last, unit), KEY_STYLES.fetch(DASHES[slot], "solid")]
        end
        tip = { "title" => "#{number(level)} clients · #{title}", "rows" => rows }
        label = ([tip["title"]] + rows.map { |_, name, value| "#{name}: #{value}" }).join(", ")

        x0 = [x.(i) - (band / 2), left].max
        x1 = [x.(i) + (band / 2), width - right].min
        svg << %(<rect class="hit" x="#{f(x0)}" y="#{top}" width="#{f(x1 - x0)}" height="#{plot_height}" data-cx="#{f(x.(i))}" tabindex="0" aria-label="#{h(label)}" data-tip="#{h(JSON.generate(tip))}"/>)
      end

      figure(title, svg, width, height)
    end

    # Ticks at 1, 2 and 5 times powers of ten (or just the powers of ten when
    # that's too many), and the matching y scale.
    def log_scale(min, max, top, plot_height)
      low = Math.log10(min).floor
      high = Math.log10(max).ceil
      candidates = (low..high).flat_map { |exponent| [1, 2, 5].map { |m| m * (10.0**exponent) } }
      candidates = (low..high).map { |exponent| 10.0**exponent } if candidates.size > 9
      first = candidates.reverse.find { |tick| tick <= min } || candidates.first
      last = candidates.find { |tick| tick >= max } || candidates.last
      ticks = candidates.select { |tick| tick >= first && tick <= last }

      range = Math.log10(last) - Math.log10(first)
      range = 1 if range.zero?
      y = ->(value) { top + plot_height - ((Math.log10(value) - Math.log10(first)) / range * plot_height) }

      [ticks, y]
    end

    # One horizontal bar per server: its label above the bar (configurations
    # are long), its value at the tip.
    def bar_chart(title, rows, metric, unit:)
      rows = rows.select { |r| metric.(r) }
      max = rows.map { |r| metric.(r) }.max || 0

      width = 600
      left = 4
      value_width = 84
      row_height = 42
      bar_height = 14
      top = 4
      axis_height = 24
      plot_width = width - left - value_width
      plot_bottom = top + (rows.size * row_height)
      height = plot_bottom + axis_height
      ticks = nice_ticks(max)
      scale = ->(value) { value.to_f / ticks.last * plot_width }

      svg = +""
      ticks.each do |tick|
        x = left + scale.(tick)
        svg << %(<line class="grid" x1="#{f(x)}" x2="#{f(x)}" y1="#{top}" y2="#{plot_bottom}"/>) unless tick.zero?
        svg << %(<text class="axis-label" x="#{f(x)}" y="#{height - 6}" text-anchor="#{tick.zero? ? "start" : "middle"}">#{h(axis_number(tick))}</text>)
      end

      rows.each_with_index do |r, i|
        y = top + (i * row_height)
        value = metric.(r)
        length = scale.(value)
        label = server_label(r)
        tip = [format_value(value, unit), label, "#{r["concurrency"]} clients"]
        bar_y = y + 20

        svg << %(<text class="row-label" x="#{left}" y="#{y + 14}">#{h(label)}</text>)
        svg << %(<path class="mark" d="#{bar_path(left, bar_y, length, bar_height)}" style="fill: var(--series-#{row_slot(r)})"/>)
        svg << %(<text class="value-label" x="#{f(left + length + 6)}" y="#{f(bar_y + bar_height - 3)}">#{h(format_value(value, unit))}</text>)
        svg << %(<rect class="hit" x="0" y="#{y}" width="#{width}" height="#{row_height}" tabindex="0" aria-label="#{h(tip.join(", "))}" data-tip="#{h(JSON.generate(tip))}"/>)
      end

      svg << %(<line class="baseline" x1="#{left}" x2="#{left}" y1="#{top}" y2="#{plot_bottom}"/>)

      figure(title, svg, width, height)
    end

    def figure(title, svg, width, height)
      @chart_id += 1
      id = "chart-#{@chart_id}"

      <<~HTML
        <figure class="chart">
          <figcaption id="#{id}">#{h(title)}</figcaption>
          <svg viewBox="0 0 #{width} #{height}" role="img" aria-labelledby="#{id}" preserveAspectRatio="xMinYMin meet">#{svg}</svg>
        </figure>
      HTML
    end

    def grid_y(ticks, x1, x2, top, plot_height, unit)
      svg = +""

      ticks.each do |tick|
        y = top + plot_height - (tick.to_f / ticks.last * plot_height)
        svg << %(<line class="grid" x1="#{x1}" x2="#{x2}" y1="#{f(y)}" y2="#{f(y)}"/>) unless tick.zero?
        svg << %(<text class="axis-label" x="#{x1 - 8}" y="#{f(y + 4)}" text-anchor="end">#{h(axis_number(tick))}</text>)
      end

      svg << %(<text class="axis-title" x="#{x1 - 8}" y="#{top - 18}" text-anchor="end">#{h(unit)}</text>)
      svg
    end

    # A column with a 4px rounded top and a square base.
    def column_path(x, base, width, height)
      radius = [4, width / 2.0, height].min
      y = base - height
      "M#{f(x)},#{f(base)}V#{f(y + radius)}Q#{f(x)},#{f(y)} #{f(x + radius)},#{f(y)}H#{f(x + width - radius)}Q#{f(x + width)},#{f(y)} #{f(x + width)},#{f(y + radius)}V#{f(base)}Z"
    end

    # A bar with a 4px rounded end and a square base.
    def bar_path(x, y, length, height)
      radius = [4, height / 2.0, length].min
      "M#{f(x)},#{f(y)}H#{f(x + length - radius)}Q#{f(x + length)},#{f(y)} #{f(x + length)},#{f(y + radius)}V#{f(y + height - radius)}Q#{f(x + length)},#{f(y + height)} #{f(x + length - radius)},#{f(y + height)}H#{f(x)}Z"
    end

    def nice_ticks(max)
      return [0, 1] unless max.positive?

      raw = max / 4.0
      magnitude = 10**Math.log10(raw).floor
      step = [1, 2, 2.5, 5, 10].map { |m| m * magnitude }.find { |s| s >= raw }
      count = (max / step).ceil
      (0..count).map { |i| i * step }
    end

    # One entry per color: the server's full label when only one server has
    # that color (e.g. column charts, which have no row labels), otherwise the
    # family names (servers in bar charts are labeled individually).
    #
    # Line charts (detailed: false) key each entry with a sample of its line,
    # including the dash pattern that tells Puma's thread counts apart.
    def legend(rows, detailed: true)
      items = rows.uniq { |r| r["server"] }.group_by { |r| row_slot(r, shades: !detailed) }.map do |slot, servers|
        text = if servers.size == 1 && detailed
          server_label(servers.first)
        elsif slot.start_with?("puma-")
          puma_name(servers.first)
        else
          servers.map { |r| (DATABASE_FREE.include?(r["scenario"]) ? PLAIN_NAMES : FAMILY_NAMES).fetch(display_family(r)) }.uniq.join(" / ")
        end

        key = if detailed
          %(<span class="swatch" style="background: var(--series-#{slot})"></span>)
        else
          dash = DASHES[slot] ? %( stroke-dasharray="#{DASHES[slot]}") : ""
          %(<svg class="line-key" width="24" height="8" aria-hidden="true"><line x1="1" y1="4" x2="23" y2="4" style="stroke: var(--series-#{slot})"#{dash}/></svg>)
        end

        %(<li>#{key}#{h(text)}</li>)
      end

      %(<ul class="legend" aria-label="Legend">#{items.join}</ul>)
    end

    # -- Tables --

    def table(rows)
      # By process count, then in the suite's server order (which pairs comparable servers), then by clients.
      order = rows.map { |r| r["server"] }.uniq
      body = rows.sort_by { |r| [r["workers"].to_i, order.index(r["server"]), r["concurrency"]] }.map do |r|
        latency = r["latency_ms"] || {}

        <<~HTML
          <tr>
            <th scope="row"><span class="swatch" style="background: var(--series-#{row_slot(r)})"></span>#{h(server_label(r))}</th>
            <td>#{h(r["concurrency"])}</td>
            <td>#{h(number(r["rps"]))}</td>
            <td>#{h(decimal(latency["p50"]))}</td>
            <td>#{h(decimal(latency["p90"]))}</td>
            <td>#{h(decimal(latency["p99"]))}</td>
            <td>#{h(r["errors"])}</td>
            <td>#{h(r["cpu_percent"])}%</td>
            <td>#{h(r["rss_mb"])}</td>
            <td>#{h(r["connections"])}</td>
            <td>#{h(r["threads_per_process"])}</td>
          </tr>
        HTML
      end

      <<~HTML
        <details class="table-view">
          <summary>Table</summary>
          <div class="table-scroll">
            <table>
              <thead><tr><th scope="col">Server</th><th scope="col">Clients</th><th scope="col">Req/s</th><th scope="col">p50 ms</th><th scope="col">p90 ms</th><th scope="col">p99 ms</th><th scope="col">Errors</th><th scope="col">CPU</th><th scope="col">Memory MB</th><th scope="col">Connections</th><th scope="col">Threads/process</th></tr></thead>
              <tbody>#{body.join}</tbody>
            </table>
          </div>
        </details>
      HTML
    end

    # -- Data --

    def suite_rows(suite, scenario)
      suite["servers"].flat_map do |server|
        suite["concurrency"].filter_map { |concurrency| row(server, scenario, concurrency) }
      end
    end

    def row(server, scenario, concurrency)
      @rows_index ||= @results.to_h { |r| [[r["server"], r["scenario"], r["concurrency"]], r] }
      @rows_index[[server, scenario, concurrency]]
    end

    def route(scenario)
      @environment.dig("routes", scenario) || ROUTES.fetch(scenario, scenario)
    end

    # The family a measurement is shown as: without a database, it's just Puma or Falcon.
    def display_family(row)
      family = row["family"] || family_of(row["server"])
      return family unless DATABASE_FREE.include?(row["scenario"])

      family.start_with?("puma") ? "puma" : "falcon-stock"
    end

    # E.g. "Puma (workers: 4, threads: 16, connection pool: 16 per worker)".
    def server_label(row)
      details = ["workers: #{row["workers"]}"]
      details << "threads: #{row["threads"]}" if row["threads"]

      unless DATABASE_FREE.include?(row["scenario"])
        details << "connection pool: #{row["pool_size"]}#{" per worker" if row["workers"].to_i > 1}"
      end

      name = DATABASE_FREE.include?(row["scenario"]) ? PLAIN_NAMES.fetch(display_family(row)) : FAMILY_NAMES.fetch(display_family(row))
      "#{name} (#{details.join(", ")})"
    end

    # The measurement for a setup, preferring the servers of the first sweep
    # suite (other suites may run other configurations of the same setup).
    def find_row(family, workers, scenario, concurrency)
      sweep_rows.find { |r| r["family"] == family && r["workers"] == workers && r["scenario"] == scenario && r["concurrency"] == concurrency }
    end

    def sweep_rows
      @sweep_rows ||= begin
        sweep = @suites.values.find { |suite| suite["concurrency"].size >= SWEEP_POINTS }
        sweep ? @results.select { |r| sweep["servers"].include?(r["server"]) } : @results
      end
    end

    def process_label(workers)
      workers == 1 ? "1 process" : "#{workers} processes"
    end

    # In line charts (shades: true), the sweep's Puma configurations get shades
    # of blue (light to dark, and a dash pattern, by thread count) instead of
    # one color. Elsewhere rows are labeled, so Puma is just blue.
    def row_slot(row, shades: false)
      family = display_family(row)
      return FAMILY_SLOTS.fetch(family) unless shades && family == "puma" && puma_thread_counts.size > 1

      "puma-#{puma_rank(row)}"
    end

    def puma_thread_counts
      @puma_thread_counts ||= sweep_rows.select { |r| display_family(r) == "puma" }.filter_map { |r| r["threads"] }.uniq.sort
    end

    # 1 (fewest threads) to 3 (most), spread over the configurations measured.
    def puma_rank(row)
      counts = puma_thread_counts
      1 + ((counts.index(row["threads"]) || 0) * 2.0 / (counts.size - 1)).round
    end

    def puma_name(row)
      name = DATABASE_FREE.include?(row["scenario"]) ? PLAIN_NAMES.fetch("puma") : FAMILY_NAMES.fetch("puma")
      "#{name} (#{row["threads"]} threads)"
    end

    # The best Puma configuration for this point in the sweep, by the block's score (lowest wins).
    def best_puma(workers, scenario, concurrency, &score)
      sweep_rows.select { |r| r["family"] == "puma" && r["workers"] == workers && r["scenario"] == scenario && r["concurrency"] == concurrency }.min_by(&score)
    end

    def cores(row)
      format("%.1f", row["cpu_percent"].to_f / 100)
    end

    def family_of(server)
      FAMILY_SLOTS.keys.sort_by { |family| -family.length }.find { |family| server.start_with?("#{family}-") } || "puma"
    end

    def slot(server)
      FAMILY_SLOTS.fetch(family_of(server))
    end

    # Requests per second per core of CPU the server used.
    def per_core(row)
      cpu = row["cpu_percent"].to_f
      row["rps"] / (cpu / 100) if cpu.positive?
    end

    def latency_label
      "#{format("%g", @environment["latency_ms"] || 0)}ms"
    end

    def json_for_script
      # Escape "</" so the data can't close the script element.
      JSON.generate("environment" => @environment, "suites" => @suites, "results" => @results).gsub("</", "<\\/")
    end

    # -- Formatting --

    def h(value)
      CGI.escapeHTML(value.to_s)
    end

    def f(value)
      format("%.1f", value)
    end

    def number(value)
      value.round.to_s.reverse.scan(/\d{1,3}/).join(",").reverse
    end

    def decimal(value)
      return "" unless value

      value >= 100 ? number(value) : format("%.2f", value)
    end

    def ms(value)
      return "" unless value

      value >= 100 ? "#{number(value)}ms" : "#{format("%.1f", value)}ms"
    end

    def format_value(value, unit)
      unit == "ms" ? ms(value) : "#{number(value)} #{unit}"
    end

    def format_metric(value, unit)
      case unit
      when "ms" then ms(value)
      when "cores" then "#{format("%.1f", value)} cores"
      else "#{number(value)} #{unit}"
      end
    end

    def axis_number(value)
      if value < 1
        format("%g", value.round(3))
      elsif value >= 1000
        value % 1000 == 0 ? "#{number(value / 1000)}k" : "#{format("%g", value / 1000.0)}k"
      else
        format("%g", value)
      end
    end

    STYLE = <<~CSS
      .viz-root {
        color-scheme: light;
        --surface-1: #fcfcfb;
        --page: #f9f9f7;
        --text-primary: #0b0b0b;
        --text-secondary: #52514e;
        --text-muted: #898781;
        --grid: #e1e0d9;
        --axis: #c3c2b7;
        --border: rgba(11, 11, 11, 0.10);
        --good: #006300;
        --bad: #d03b3b;
        --series-1: #2a78d6;
        --series-2: #eb6834;
        --series-3: #1baf7a;
        --series-neutral: #898781;
        /* Puma by thread count: an ordinal blue ramp, light to dark. */
        --series-puma-1: #86b6ef;
        --series-puma-2: #2a78d6;
        --series-puma-3: #104281;
      }
      @media (prefers-color-scheme: dark) {
        :root:where(:not([data-theme="light"])) .viz-root {
          color-scheme: dark;
          --surface-1: #1a1a19;
          --page: #0d0d0d;
          --text-primary: #ffffff;
          --text-secondary: #c3c2b7;
          --text-muted: #898781;
          --grid: #2c2c2a;
          --axis: #383835;
          --border: rgba(255, 255, 255, 0.10);
          --good: #0ca30c;
          --bad: #e66767;
          --series-1: #3987e5;
          --series-2: #d95926;
          --series-3: #199e70;
          --series-neutral: #8f8d86;
          --series-puma-1: #184f95;
          --series-puma-2: #3987e5;
          --series-puma-3: #86b6ef;
        }
        :root:where(:not([data-theme="light"])) body { background: #0d0d0d; }
      }
      :root[data-theme="dark"] .viz-root {
        color-scheme: dark;
        --surface-1: #1a1a19;
        --page: #0d0d0d;
        --text-primary: #ffffff;
        --text-secondary: #c3c2b7;
        --text-muted: #898781;
        --grid: #2c2c2a;
        --axis: #383835;
        --border: rgba(255, 255, 255, 0.10);
        --good: #0ca30c;
        --bad: #e66767;
        --series-1: #3987e5;
        --series-2: #d95926;
        --series-3: #199e70;
        --series-neutral: #8f8d86;
        --series-puma-1: #184f95;
        --series-puma-2: #3987e5;
        --series-puma-3: #86b6ef;
      }
      :root[data-theme="dark"] body { background: #0d0d0d; }
      * { box-sizing: border-box; }
      body { margin: 0; background: #f9f9f7; font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
      .viz-root { background: var(--page); color: var(--text-primary); max-width: 1240px; margin: 0 auto; padding: 32px 16px 64px; min-height: 100vh; }
      h1 { font-size: 28px; margin: 0 0 4px; font-weight: 650; }
      h2 { font-size: 21px; margin: 0 0 4px; font-weight: 650; }
      h3 { font-size: 16px; margin: 0; font-weight: 600; }
      h3 code.route { font-size: 15px; background: none; border: 0; padding: 0; font-weight: 600; }
      p { margin: 0 0 8px; }
      code { font-size: 13px; background: var(--surface-1); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px; overflow-wrap: anywhere; }
      .page-header { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 24px; }
      .lede { color: var(--text-secondary); max-width: 70ch; }
      .meta { color: var(--text-muted); font-size: 13px; }
      .theme-toggle, .download { font: inherit; font-size: 13px; color: var(--text-primary); background: var(--surface-1); border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px; cursor: pointer; }
      .highlights { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 24px; }
      .tile { background: var(--surface-1); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
      .tile-label { color: var(--text-secondary); font-size: 13px; }
      .tile-value { font-size: 28px; font-weight: 600; margin: 2px 0; }
      .tile-delta { font-size: 13px; font-weight: 600; }
      .tile-delta.good { color: var(--good); }
      .tile-delta.bad { color: var(--bad); }
      .tile-note { color: var(--text-muted); font-size: 12px; margin-top: 2px; }
      .toc { display: flex; flex-wrap: wrap; gap: 4px 16px; margin-bottom: 8px; font-size: 14px; }
      .toc a { color: var(--text-secondary); }
      .suite { margin-top: 40px; }
      .section-lede { color: var(--text-secondary); max-width: 80ch; }
      .legend { list-style: none; display: flex; flex-wrap: wrap; gap: 4px 18px; padding: 0; margin: 12px 0 0; font-size: 13px; color: var(--text-secondary); }
      .legend li { display: flex; align-items: center; gap: 6px; }
      .line-key line { stroke-width: 2; stroke-linecap: round; }
      .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: baseline; }
      .legend .swatch { margin-right: 0; }
      .scenario { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 16px; margin-top: 16px; }
      .scenario-description { color: var(--text-secondary); font-size: 14px; }
      .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 420px), 1fr)); gap: 8px 24px; }
      .chart { margin: 8px 0 0; min-width: 0; }
      .chart figcaption { font-size: 13px; color: var(--text-secondary); margin-bottom: 4px; }
      .chart svg { width: 100%; height: auto; display: block; overflow: visible; }
      .grid { stroke: var(--grid); stroke-width: 1; }
      .baseline { stroke: var(--axis); stroke-width: 1; }
      .axis-label, .axis-title { fill: var(--text-muted); font-size: 12px; font-variant-numeric: tabular-nums; }
      .row-label { fill: var(--text-secondary); font-size: 12px; }
      .value-label { fill: var(--text-primary); font-size: 12px; font-variant-numeric: tabular-nums; }
      .mark { transition: opacity 120ms; }
      .series-line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
      .marker { stroke: var(--surface-1); stroke-width: 2; }
      .crosshair { stroke: var(--axis); stroke-width: 1; visibility: hidden; }
      svg.crosshairing .crosshair { visibility: visible; }
      .hit { fill: transparent; cursor: default; outline: none; }
      svg.hovering .mark { opacity: 0.55; }
      svg.hovering .mark.active { opacity: 1; }
      .table-view { margin-top: 12px; }
      .table-view summary { cursor: pointer; color: var(--text-secondary); font-size: 13px; }
      .table-scroll { overflow-x: auto; }
      table { border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 8px; font-variant-numeric: tabular-nums; }
      th, td { text-align: right; padding: 5px 8px; border-bottom: 1px solid var(--grid); white-space: nowrap; }
      th:first-child { text-align: left; font-weight: 500; }
      thead th { color: var(--text-muted); font-weight: 500; }
      .notes { color: var(--text-secondary); max-width: 90ch; padding-left: 18px; }
      .env { display: grid; grid-template-columns: max-content 1fr; gap: 4px 16px; font-size: 14px; }
      .env dt { color: var(--text-muted); }
      .env dd { margin: 0; overflow-wrap: anywhere; }
      .tooltip { position: fixed; z-index: 10; pointer-events: none; background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--border); border-radius: 8px; padding: 6px 10px; font: 13px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif; box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12); }
      .tooltip strong { display: block; font-size: 14px; }
      .tooltip span { display: block; color: var(--text-secondary); }
      .tooltip .tip-title { color: var(--text-secondary); margin-bottom: 4px; }
      .tooltip .tip-row { display: grid; grid-template-columns: 14px auto 1fr; gap: 0 8px; align-items: baseline; }
      .tooltip .tip-row .key { display: inline-block; width: 14px; height: 0; border-top: 2px solid; align-self: center; }
      .tooltip .tip-row strong { display: inline; font-size: 13px; color: var(--text-primary); font-variant-numeric: tabular-nums; }
      .tooltip .tip-row span { display: inline; }
      .sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); }
      @media (max-width: 640px) {
        .page-header { flex-direction: column; }
        .tile-value { font-size: 24px; }
      }
    CSS

    SCRIPT = <<~JS
      (function () {
        var tooltip = document.querySelector(".tooltip");
        var root = document.documentElement;

        function render(tip) {
          tooltip.textContent = "";

          if (Array.isArray(tip)) {
            // Bars: value first, then what it is.
            var value = document.createElement("strong");
            value.textContent = tip[0];
            tooltip.appendChild(value);
            tip.slice(1).forEach(function (line) {
              var span = document.createElement("span");
              span.textContent = line;
              tooltip.appendChild(span);
            });
            return;
          }

          // Lines: every series at the hovered level, keyed by a short stroke of its color.
          var title = document.createElement("div");
          title.className = "tip-title";
          title.textContent = tip.title;
          tooltip.appendChild(title);
          tip.rows.forEach(function (row) {
            var line = document.createElement("div");
            line.className = "tip-row";
            var key = document.createElement("i");
            key.className = "key";
            key.style.borderTopColor = "var(--series-" + row[0] + ")";
            key.style.borderTopStyle = row[3] || "solid";
            var value = document.createElement("strong");
            value.textContent = row[2];
            var name = document.createElement("span");
            name.textContent = row[1];
            line.appendChild(key);
            line.appendChild(value);
            line.appendChild(name);
            tooltip.appendChild(line);
          });
        }

        function show(target, x, y) {
          render(JSON.parse(target.getAttribute("data-tip")));
          tooltip.hidden = false;
          var box = tooltip.getBoundingClientRect();
          var left = Math.min(x + 14, window.innerWidth - box.width - 8);
          var top = y + 14 + box.height > window.innerHeight ? y - box.height - 10 : y + 14;
          tooltip.style.left = Math.max(8, left) + "px";
          tooltip.style.top = Math.max(8, top) + "px";

          var svg = target.ownerSVGElement;
          var cx = target.getAttribute("data-cx");
          if (cx) {
            var crosshair = svg.querySelector(".crosshair");
            crosshair.setAttribute("x1", cx);
            crosshair.setAttribute("x2", cx);
            svg.classList.add("crosshairing");
            return;
          }

          var mark = target.previousElementSibling;
          while (mark && !mark.classList.contains("mark")) mark = mark.previousElementSibling;
          svg.classList.add("hovering");
          svg.querySelectorAll(".mark.active").forEach(function (m) { m.classList.remove("active"); });
          if (mark) mark.classList.add("active");
        }

        function hide(target) {
          tooltip.hidden = true;
          var svg = target && target.ownerSVGElement;
          if (svg) {
            svg.classList.remove("crosshairing");
            svg.classList.remove("hovering");
            svg.querySelectorAll(".mark.active").forEach(function (m) { m.classList.remove("active"); });
          }
        }

        document.querySelectorAll(".hit").forEach(function (hit) {
          hit.addEventListener("pointermove", function (event) { show(hit, event.clientX, event.clientY); });
          hit.addEventListener("pointerleave", function () { hide(hit); });
          hit.addEventListener("focus", function () {
            var box = hit.getBoundingClientRect();
            show(hit, box.left + box.width / 2, box.top);
          });
          hit.addEventListener("blur", function () { hide(hit); });
        });

        try {
          var saved = localStorage.getItem("async-mongo-bench-theme");
          if (saved) root.setAttribute("data-theme", saved);
        } catch (e) {}

        document.querySelector(".theme-toggle").addEventListener("click", function () {
          var dark = root.getAttribute("data-theme") === "dark" ||
            (!root.getAttribute("data-theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
          var next = dark ? "light" : "dark";
          root.setAttribute("data-theme", next);
          try { localStorage.setItem("async-mongo-bench-theme", next); } catch (e) {}
        });

        document.querySelector(".download").addEventListener("click", function () {
          var data = document.getElementById("results-data").textContent;
          var link = document.createElement("a");
          link.href = URL.createObjectURL(new Blob([data], { type: "application/json" }));
          link.download = "async-mongo-benchmark.json";
          link.click();
          URL.revokeObjectURL(link.href);
        });
      })();
    JS
  end
end
