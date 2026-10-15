# frozen_string_literal: true

module Async
  module Mongo
    # Tracks a pool's connections: idle ones (a stack, so the most recently used
    # is reused first), in-use ones, how many are being established, and the
    # fibers waiting for one.
    #
    # Waiters are served first come, first served: while anyone is waiting, new
    # requests queue behind them, and each released connection (or freed
    # connecting slot) is handed directly to the oldest waiter, waking only that
    # fiber. Only establishing connections is limited (to `max_connecting` at a
    # time), and connections being established count toward `limit`.
    #
    # Not thread-safe: the state must only be changed from the pool's owner
    # thread (where all of its fibers run). Other threads may only set {#error}
    # and then call {#signal}.
    class Controller
      # A fiber waiting in {#acquire}. It's granted either a connection or a
      # reserved connecting slot (`create`).
      Waiter = Struct.new(:condition, :connection, :create) do
        def granted?
          connection || create
        end
      end

      # @parameter limit [Integer | Nil] Maximum number of connections (idle + in use + connecting).
      # @parameter max_connecting [Integer] Maximum number of connections being established at once.
      # @parameter wait_timeout [Numeric | Nil] Default maximum time {#acquire} waits for a connection.
      # @parameter address [Object] Server address, for error messages.
      # @yields {|context| ...} Establishes and returns a new connection.
      def initialize(limit:, max_connecting:, wait_timeout:, address:, &constructor)
        @constructor = constructor
        @limit = limit
        @max_connecting = max_connecting
        @wait_timeout = wait_timeout
        @address = address

        @idle = []
        @in_use = {}.compare_by_identity
        @connecting = 0
        @error = nil
        # Nil until {#take_peak_in_use} is first called.
        @peak_in_use = nil

        # Guards @waiters and grants, which other threads read when signalling.
        @mutex = Thread::Mutex.new
        @waiters = []
        @dispatching = false
        @dispatch_again = false
      end

      # @attribute [Exception | Nil] When set, {#acquire} raises it (e.g. the pool was cleared or closed).
      attr_accessor :error

      attr_reader :limit, :max_connecting

      def size
        @idle.size + @in_use.size
      end

      def idle_count
        @idle.size
      end

      def in_use_count
        @in_use.size
      end

      def connecting_count
        @connecting
      end

      def waiting_count
        @waiters.size
      end

      def in_use?(connection)
        @in_use.key?(connection)
      end

      # @returns [Array(Connection)] A snapshot of the connections in use.
      def in_use
        @in_use.keys
      end

      # The most connections in use at once since the last call, after which the
      # count restarts from the current number in use. Tracking starts with the
      # first call (which returns the current number), so a pool that's never
      # measured doesn't track it.
      #
      # @returns [Integer]
      def take_peak_in_use
        peak = [@peak_in_use || 0, @in_use.size].max
        @peak_in_use = @in_use.size
        peak
      end

      # Wake all waiting fibers so they notice {#error}. Safe to call from any thread.
      def signal
        @mutex.synchronize do
          @waiters.each { |waiter| waiter.condition.signal }
        end
      end

      # Take an idle connection or establish a new one, waiting in line if
      # neither is possible right now.
      #
      # @parameter timeout [Numeric | Nil] Maximum time to wait (defaults to wait_timeout).
      # @parameter context [Mongo::Operation::Context | Nil] Passed to the constructor.
      # @raises [Exception] {#error}, if set.
      # @raises [Mongo::Error::ConnectionCheckOutTimeout] After waiting for longer than the timeout.
      def acquire(timeout: @wait_timeout, context: nil)
        raise @error if @error

        # Only skip the line when there is no line, so new requests can't barge ahead of waiting ones.
        if @waiters.empty?
          if (connection = acquire_idle)
            return connection
          end

          return create(context) if creatable?
        end

        waiter = wait_in_line(timeout)
        waiter.connection || create(context, reserved: true)
      end

      # Return a connection. It becomes idle (or goes straight to the next
      # waiter) unless it has a close_reason, in which case it's disconnected
      # with that reason.
      #
      # @returns [Boolean] Whether the connection belonged to this controller.
      def release(connection)
        return false unless @in_use.key?(connection)

        # close_reason may context switch (the generation manager takes a lock),
        # so keep the connection counted as in use until it's decided.
        reason = connection.close_reason
        @in_use.delete(connection)

        if reason
          retire(connection, reason)
        else
          @idle.push(connection)
        end

        dispatch
        true
      end

      # Establish a connection and leave it idle, as long as nobody is waiting
      # (waiters are given connecting slots directly) and there are fewer than
      # `max` connections (including ones being established).
      #
      # @returns [Boolean] Whether a connection was created.
      def create_idle(max = @limit)
        return false unless @waiters.empty? && creatable?(max)

        create(idle: true)
        true
      end

      # Disconnect idle connections, optionally only those the block selects.
      #
      # @returns [Integer] The number of connections retired.
      #
      # @yields {|connection| ...} Returns the close reason for connections to retire (nil to keep them).
      def retire_idle(reason = :idle)
        retired = 0

        @idle.dup.each do |connection|
          connection_reason = block_given? ? yield(connection) : reason
          next unless connection_reason
          # The block may context switch, so the connection may have been taken in the meantime.
          next unless @idle.delete(connection)

          retire(connection, connection_reason)
          retired += 1
        end

        # Freed capacity may let a waiter establish a connection.
        dispatch if retired > 0
        retired
      end

      # Disconnect idle connections that can't be handed out again (stale
      # generation, idle for too long, closed, inherited across a fork).
      def retire_unviable
        retire_idle(&:unviable_reason)
      end

      def to_s
        "#<#{self.class} idle=#{idle_count} in_use=#{in_use_count} connecting=#{connecting_count} waiting=#{waiting_count} limit=#{@limit || "∞"}>"
      end

      private

      def creatable?(max = @limit)
        return false if @connecting >= @max_connecting

        max.nil? || size + @connecting < max
      end

      def mark_in_use(connection)
        @in_use[connection] = true
        @peak_in_use = @in_use.size if @peak_in_use && @in_use.size > @peak_in_use
      end

      def acquire_idle
        while (connection = @idle.pop)
          # Count it as in use before unviable_reason (which may context switch), so it
          # can't be handed out twice and the limit stays accurate.
          mark_in_use(connection)
          reason = connection.unviable_reason
          return connection unless reason

          @in_use.delete(connection)
          retire(connection, reason)
        end

        nil
      end

      # @parameter reserved [Boolean] Whether a connecting slot was already reserved for us (granted to a waiter).
      # @parameter idle [Boolean] Leave the connection idle (for population) rather than in use.
      def create(context = nil, reserved: false, idle: false)
        @connecting += 1 unless reserved
        connection = @constructor.call(context)

        # Placed before dispatching (below), so a waiter takes this connection
        # instead of being granted a slot to establish another one.
        if idle
          @idle.push(connection)
        else
          mark_in_use(connection)
        end

        connection
      ensure
        @connecting -= 1
        # A connecting slot is free again (and the attempt may have failed).
        dispatch
      end

      # Hand idle connections and free connecting slots to waiters, oldest first.
      def dispatch
        # acquire_idle can context switch, letting another fiber release a
        # connection and dispatch re-entrantly; have the running dispatch loop again instead.
        if @dispatching
          @dispatch_again = true
          return
        end

        @dispatching = true

        begin
          @dispatch_again = false

          until @waiters.empty?
            if (connection = acquire_idle)
              # The waiter we were serving may have given up while acquire_idle context switched.
              if @waiters.empty?
                @in_use.delete(connection)
                @idle.push(connection)
                break
              end

              grant(connection: connection)
            elsif creatable?
              @connecting += 1
              grant(create: true)
            else
              break
            end
          end
        end while @dispatch_again # Runs at least once.
      ensure
        @dispatching = false
      end

      def grant(connection: nil, create: false)
        @mutex.synchronize do
          waiter = @waiters.shift
          waiter.connection = connection
          waiter.create = create
          waiter.condition.signal
        end
      end

      def wait_in_line(timeout)
        deadline = timeout ? monotonic_time + timeout : nil
        waiter = Waiter.new(Thread::ConditionVariable.new)

        @mutex.synchronize { @waiters.push(waiter) }
        # The state may already allow serving the line (e.g. we were the first in it).
        dispatch

        until waiter.granted?
          raise @error if @error

          remaining = deadline && deadline - monotonic_time
          raise checkout_timeout(timeout) if remaining && remaining <= 0

          @mutex.synchronize do
            # Checked while holding the mutex, which grants and other threads' signals also take, so neither can be missed.
            waiter.condition.wait(@mutex, remaining) unless waiter.granted? || @error
          end
        end

        waiter
      rescue Exception
        give_up(waiter)
        raise
      end

      # Leave the line (timeout, error or cancellation), passing on anything we were granted.
      def give_up(waiter)
        @mutex.synchronize { @waiters.delete(waiter) }

        if (connection = waiter.connection)
          waiter.connection = nil
          release(connection)
        elsif waiter.create
          waiter.create = false
          @connecting -= 1
          dispatch
        end
      end

      def checkout_timeout(timeout)
        ::Mongo::Error::ConnectionCheckOutTimeout.new(
          "Timed out attempting to check out a connection from pool for #{@address} after #{timeout} sec " \
            "(#{idle_count} idle, #{in_use_count} in use, #{connecting_count} connecting, #{waiting_count} waiting, max size: #{@limit || "unlimited"})",
          address: @address
        )
      end

      def retire(connection, reason)
        connection.disconnect!(reason: reason)
      rescue StandardError
        # The connection is being discarded either way.
      end

      def monotonic_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
