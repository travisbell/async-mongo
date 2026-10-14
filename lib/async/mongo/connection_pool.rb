# frozen_string_literal: true

module Async
  module Mongo
    # A drop-in replacement for Mongo::Server::ConnectionPool, native to the
    # Async event loop.
    #
    # Connections are checked out and in by fibers on a single thread, the
    # pool's *owner* (the first thread that checks out from inside an Async
    # task). Checkouts from anywhere else (plain threads, blocking code, a
    # second reactor thread) are delegated to a stock driver pool that's
    # created on first use, so installing this is safe in any process.
    #
    # The driver's background threads (SDAM monitor, socket reaper) only call
    # #ready, #clear, #close and #close_idle_sockets. Those just flip flags,
    # wake waiting fibers and signal a maintenance fiber, which does the real
    # work on the owner thread: keeping min_size connections, retiring idle
    # and stale connections, and interrupting in-use connections when the
    # server goes away.
    class ConnectionPool
      include ::Mongo::Loggable
      include ::Mongo::Monitoring::Publishable
      extend Forwardable

      DEFAULT_MAX_SIZE = 20
      DEFAULT_MIN_SIZE = 0
      DEFAULT_WAIT_TIMEOUT = 10
      DEFAULT_MAX_CONNECTING = 2

      # How often the maintenance fiber wakes up when nothing signals it.
      MAINTENANCE_INTERVAL = 5

      attr_reader :server, :options, :generation_manager

      def_delegators :server, :address, :monitoring
      def_delegators :generation_manager, :generation, :generation_unlocked

      def initialize(server, options = {})
        @server = server
        @raw_options = options
        @options = normalize_options(options).freeze
        @generation_manager = ::Mongo::Server::ConnectionPool::GenerationManager.new(server: server)

        @ready = false
        @closed = false

        @owner_thread = nil
        @owner_lock = Thread::Mutex.new
        @fallback_pool = nil

        @maintenance_task = nil
        @maintenance_stop = false
        @maintenance_wakeup = ::Mongo::Semaphore.new
        @interrupt_pending = false
        @retire_stale_pending = false
        @retire_idle_pending = false
        # Connections being established (owner thread only).
        @pending = Set.new.compare_by_identity
        @disconnect_in_use_pending = false

        @controller = Controller.new(
          limit: max_size.zero? ? nil : max_size,
          max_connecting: max_connecting,
          wait_timeout: wait_timeout,
          address: address
        ) do |context|
          create_connection(context)
        end

        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::PoolCreated.new(address, @options, self))
      end

      # -- Configuration --

      def max_size
        @options[:max_size] || DEFAULT_MAX_SIZE
      end

      def min_size
        @options[:min_size] || DEFAULT_MIN_SIZE
      end

      # @parameter context [Mongo::Operation::Context | Nil] With client-side operation timeouts (timeoutMS), the time remaining.
      def wait_timeout(context = nil)
        context&.remaining_timeout_sec || @options[:wait_timeout] || DEFAULT_WAIT_TIMEOUT
      end

      def max_idle_time
        @options[:max_idle_time]
      end

      def max_connecting
        @options[:max_connecting] || DEFAULT_MAX_CONNECTING
      end

      # -- State --

      def closed?
        @closed
      end

      def ready?
        @ready
      end

      # Raises PoolClosedError on a closed pool, like the stock pool.
      def paused?
        raise_if_closed!
        !@ready
      end

      def state
        if @closed
          :closed
        elsif @ready
          :ready
        else
          :paused
        end
      end

      # Raises PoolClosedError on a closed pool, like the stock pool.
      def size
        raise_if_closed!
        @controller.size + (@fallback_pool&.size || 0)
      end

      # Raises PoolClosedError on a closed pool, like the stock pool.
      def available_count
        raise_if_closed!
        @controller.idle_count + (@fallback_pool&.available_count || 0)
      end

      # Connections checked out or being established, plus checkout requests waiting for one.
      def unavailable_connections
        @controller.in_use_count + @controller.connecting_count + @controller.waiting_count + (@fallback_pool&.unavailable_connections || 0)
      end

      # The populate semaphore, signalled to wake the maintenance fiber (the stock pool's populator).
      def populate_semaphore
        @maintenance_wakeup
      end

      # @returns [Hash] Connection counts for the async pool and (if it was needed) the fallback pool.
      def stats
        {
          state: state,
          idle: @controller.idle_count,
          in_use: @controller.in_use_count,
          connecting: @controller.connecting_count,
          waiting: @controller.waiting_count,
          max_size: max_size,
          min_size: min_size,
          maintenance: maintenance_running?,
          fallback: @fallback_pool && {
            size: @fallback_pool.size,
            available: @fallback_pool.available_count,
            unavailable: @fallback_pool.unavailable_connections
          }
        }
      end

      def summary
        fallback = @fallback_pool ? " fallback=#{@fallback_pool.summary}" : ""

        "#<#{self.class} size=#{@controller.size} idle=#{@controller.idle_count} " \
          "in_use=#{@controller.in_use_count} connecting=#{@controller.connecting_count} waiting=#{@controller.waiting_count} " \
          "max_size=#{max_size} min_size=#{min_size} state=#{state}#{fallback}>"
      end

      def inspect
        summary
      end

      # Driver compatibility: Lint calls this on the stock pool.
      def check_invariants; end

      def populator
        @populator ||= PopulatorShim.new(self)
      end

      # -- Checkout / checkin --

      def check_out(connection_global_id: nil, context: nil)
        return fallback_pool.check_out(connection_global_id: connection_global_id, context: context) unless async_context?

        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutStarted.new(address))

        raise_if_closed!
        raise_if_paused!
        ensure_maintenance

        connection = @controller.acquire(timeout: wait_timeout(context), context: context)

        begin
          if (error = @controller.error)
            raise error
          end

          publish_cmap_event(::Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut.new(address, connection.id, self))
        rescue Exception
          # Hand the connection back if we fail (or are cancelled) before the caller owns it, otherwise its slot leaks.
          @controller.release(connection)
          raise
        end

        connection
      rescue ::Mongo::Error, ::Mongo::Error::ConnectionCheckOutTimeout => e
        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed.new(address, checkout_failed_reason(e)))
        raise
      end

      def check_in(connection)
        unless own?(connection)
          return @fallback_pool.check_in(connection) if @fallback_pool && connection.connection_pool.equal?(@fallback_pool)

          raise ArgumentError, "Trying to check in a connection which was not checked out by this pool: #{connection} checked out from pool #{connection.connection_pool} (for #{self})"
        end

        do_check_in(connection)
      end

      # Same rules as the stock pool: the ConnectionCheckedIn event comes first,
      # then the connection is either made available or closed with a reason.
      def do_check_in(connection)
        unless @controller.in_use?(connection)
          # An interrupted connection is disconnected while in use; ignore its late check-in, like the stock pool.
          return if connection.closed? && connection.interrupted?

          raise ArgumentError, "Trying to check in a connection which is not currently checked out by this pool: #{connection} (for #{self})"
        end

        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::ConnectionCheckedIn.new(address, connection.id, self))
        connection.checked_in!
        @controller.release(connection)
        # A closed connection leaves room to repopulate to min_size.
        @maintenance_wakeup.signal if connection.closed?
      end

      # Connections are only pinned in load-balanced mode, which uses the stock pool.
      def check_out_pinned_connection(_connection_global_id)
        nil
      end

      def with_connection(connection_global_id: nil, context: nil, &block)
        if async_context?
          with_async_connection(context, &block)
        else
          fallback_pool.with_connection(connection_global_id: connection_global_id, context: context, &block)
        end
      end

      # -- Lifecycle --

      def ready
        raise_if_closed!
        @fallback_pool&.ready
        return if @ready

        @ready = true
        @controller.error = nil
        @controller.signal

        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::PoolReady.new(address, @options, self))

        # Start populating to min_size, like the stock pool's populator (no-op
        # off the reactor, e.g. when the SDAM monitor thread re-readies the pool).
        ensure_maintenance if async_context?
        @maintenance_wakeup.signal
      end

      def pause
        do_pause
      end

      def do_pause
        @fallback_pool&.pause
        return unless @ready

        @ready = false
        # Waiting checkouts fail, as with the stock pool.
        @controller.error = ::Mongo::Error::PoolClearedError.new(address, self)
        @controller.signal
      end

      # Same semantics as the stock pool: bump the generation, close stale idle
      # connections (unless lazy), interrupt in-use and pending connections if
      # asked, and, if the pool was ready, publish ConnectionPoolCleared and
      # pause it when the server is unknown.
      #
      # May be called from the SDAM monitor thread. Only the owner thread
      # touches connections, so from other threads closing and interrupting is
      # done by the maintenance fiber right after.
      def clear(options = nil)
        raise_if_closed!
        do_clear(options)
      end

      def do_clear(options = nil)
        options ||= {}
        service_id = options[:service_id]

        if @fallback_pool
          # Shares our generation manager, so this bumps the generation for both pools.
          @fallback_pool.clear(options)
        else
          @generation_manager.bump(service_id: service_id)
        end

        unless options[:lazy]
          if owner_thread?
            retire_stale_idle
          else
            @retire_stale_pending = true
          end
        end

        @interrupt_pending = true if options[:interrupt_in_use_connections]

        if @ready
          publish_cmap_event(
            ::Mongo::Monitoring::Event::Cmap::PoolCleared.new(
              address,
              service_id: service_id,
              interrupt_in_use_connections: options[:interrupt_in_use_connections]
            )
          )

          # Only pause if the server was marked unknown; otherwise retries can use the ready pool.
          do_pause if @server.unknown?
        end

        @controller.signal
        @maintenance_wakeup.signal
        true
      end

      def disconnect!(options = nil)
        clear(options)
      rescue ::Mongo::Error::PoolClosedError
        # disconnect! is allowed on a closed pool.
        true
      end

      # @parameter options [Hash | Nil] :stay_ready keeps the pool usable (the
      #   driver passes it from Server#close). :force also disconnects
      #   connections that are in use.
      def close(options = nil)
        return true if @closed

        options ||= {}
        @fallback_pool&.close(options)

        unless options[:stay_ready]
          @closed = true
          @ready = false
          @controller.error = ::Mongo::Error::PoolClosedError.new(address, self)
        end

        @controller.signal

        # Connections in use are disconnected when they're checked in, since
        # they're no longer reusable once the pool is closed.
        if owner_thread?
          stop_maintenance
          @controller.retire_idle(:pool_closed)
          disconnect_in_use(:pool_closed) if options[:force]
        else
          # Only the owner thread may touch the connections; the maintenance fiber does it on the way out.
          @retire_idle_pending = true
          @disconnect_in_use_pending = true if options[:force]
          stop_maintenance
        end

        @generation_manager.close_all_pipes
        publish_cmap_event(::Mongo::Monitoring::Event::Cmap::PoolClosed.new(address, self))

        true
      end

      # Called periodically by the driver's socket reaper thread. Connections
      # idle for longer than max_idle_time are retired right away from the
      # owner thread, otherwise by the maintenance fiber.
      def close_idle_sockets
        return if @closed

        @fallback_pool&.close_idle_sockets
        return unless max_idle_time

        if owner_thread?
          @controller.retire_unviable
        else
          @maintenance_wakeup.signal
        end
      end

      # Establish one connection if there are fewer than min_size.
      #
      # @returns [Boolean] Whether a connection was created.
      # Like the stock pool, a socket error is retried once.
      def populate
        return false if @closed || !@ready

        begin
          return populate_one
        rescue ::Mongo::Error::SocketError, ::Mongo::Error::SocketTimeoutError => e
          log_warn("Populator failed to connect a connection for #{address}: #{e.class}: #{e}. It will retry.")
        end

        populate_one
      end

      def stop_populator
        @fallback_pool&.stop_populator
        stop_maintenance
        # Like the stock pool, give up on connections still being established.
        @pending.each(&:disconnect!) if owner_thread?
      end

      private

      def populate_one
        return false if @closed || !@ready || @controller.size >= min_size

        @controller.create_idle(max_size.zero? ? min_size : [min_size, max_size].min)
      end

      def retire_stale_idle
        @retire_stale_pending = false
        @controller.retire_idle do |connection|
          :stale if connection.generation < @generation_manager.generation(service_id: connection.service_id)
        end
      end

      def normalize_options(options)
        options = options.dup

        [[:min_size, :min_pool_size], [:max_size, :max_pool_size], [:wait_timeout, :wait_queue_timeout]].each do |name, alias_name|
          next unless options.key?(alias_name)

          value = options.delete(alias_name)
          if options[name] && value && options[name] != value
            raise ArgumentError, "#{name} #{options[name]} is not identical to #{alias_name} #{value}"
          end

          options[name] ||= value
        end

        if options[:min_size] && options[:max_size] && options[:max_size] != 0 && options[:min_size] > options[:max_size]
          raise ArgumentError, "Cannot have min size #{options[:min_size]} exceed max size #{options[:max_size]}"
        end

        options
      end

      # Whether the current fiber should use the async pool: it must be an Async
      # task on the owner thread, and not inside Fiber.blocking (where waiting
      # would block the thread). The first such thread becomes the owner; a new
      # one is adopted if the owner has died with nothing checked out.
      def async_context?
        return false unless ::Async::Task.current?
        return false if Fiber.current.blocking?

        owner = @owner_thread
        return true if owner.equal?(Thread.current)

        @owner_lock.synchronize do
          owner = @owner_thread

          if owner.nil? || (!owner.alive? && @controller.in_use_count.zero?)
            @owner_thread = Thread.current
          end

          @owner_thread.equal?(Thread.current)
        end
      end

      def owner_thread?
        @owner_thread.nil? || @owner_thread.equal?(Thread.current)
      end

      def own?(connection)
        connection.is_a?(Connection) && connection.connection_pool.equal?(self)
      end

      # A stock driver pool for checkouts outside the owner thread's event loop.
      # It shares our generation manager, so connection generations stay
      # consistent for SDAM (Server#unknown! compares against our generation).
      def fallback_pool
        @fallback_pool || @owner_lock.synchronize do
          @fallback_pool ||= begin
            raise_if_closed!

            options = @raw_options.except(:min_pool_size).merge(min_size: 0)
            ::Mongo::Server::ConnectionPool.new(@server, options).tap do |pool|
              pool.instance_variable_set(:@generation_manager, @generation_manager)
              pool.ready if @ready
            end
          end
        end
      end

      def with_async_connection(context)
        connection = check_out(context: context)
        yield connection
      rescue ::Mongo::Error::SocketError, ::Mongo::Error::SocketTimeoutError, ::Mongo::Error::ConnectionPerished => e
        maybe_raise_pool_cleared!(connection, e)
      rescue Exception => e
        # Anything that isn't a Mongo::Error (Async::Cancel, Timeout::Error,
        # Interrupt, ...) can arrive in the middle of a wire protocol exchange
        # and leave unread reply bytes on the socket, so never reuse the
        # connection.
        connection&.disconnect!(reason: :error) unless e.is_a?(::Mongo::Error)
        raise
      ensure
        check_in(connection) if connection && !connection.pinned? && @controller.in_use?(connection)
      end

      def create_connection(context)
        raise_if_closed!
        raise_if_paused!

        pipe, = @generation_manager.pipe_fds(service_id: @server.description.service_id)
        connection = Connection.new(@server, @options.merge(connection_pool: self, pipe: pipe, generation: generation))
        @pending.add(connection)

        begin
          connection.connect!(context)
        rescue Exception => e
          # Same as the stock pool: a failure to establish a connection marks the
          # server unknown (which clears the pool), except for overload errors.
          if e.is_a?(::Mongo::Error) && !e.label?("SystemOverloadedError")
            @server.unknown!(
              generation: e.generation,
              service_id: e.service_id,
              stop_push_monitor: true,
              network_error: e.is_a?(::Mongo::Error::SocketError) || e.is_a?(::Mongo::Error::SocketTimeoutError)
            )
          end

          # Also on cancellation, so a half-established connection doesn't leak its socket.
          connection.disconnect!(reason: :error)
          raise
        ensure
          @pending.delete(connection)
        end

        connection
      end

      def maybe_raise_pool_cleared!(connection, error)
        # The connection is nil when check_out itself failed (e.g. connect! raised).
        raise error unless connection
        raise pool_cleared_error(error) if connection.interrupted?

        if connection.generation != @generation_manager.generation(service_id: connection.service_id)
          check_in(connection)
          raise pool_cleared_error(error)
        end

        raise error
      end

      def pool_cleared_error(error)
        ::Mongo::Error::PoolClearedError.new(address, self).tap do |pool_cleared_error|
          error.labels.each { |label| pool_cleared_error.add_label(label) } if error.respond_to?(:labels)
        end
      end

      def raise_if_closed!
        raise ::Mongo::Error::PoolClosedError.new(address, self) if @closed
      end

      def raise_if_paused!
        raise ::Mongo::Error::PoolClearedError.new(address, self) unless @ready
      end

      def checkout_failed_reason(error)
        case error
        when ::Mongo::Error::PoolClosedError
          ::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed::POOL_CLOSED
        when ::Mongo::Error::ConnectionCheckOutTimeout
          ::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed::TIMEOUT
        else
          ::Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed::CONNECTION_ERROR
        end
      end

      def disconnect_in_use(reason)
        @controller.in_use.each { |connection| connection.disconnect!(reason: reason) }
      end

      # -- Maintenance fiber --

      def maintenance_running?
        @maintenance_task&.alive? || false
      end

      # Start the maintenance fiber on the current reactor unless it's already
      # running. It dies with its reactor (e.g. at the end of a Sync block), so
      # this is checked on every checkout.
      def ensure_maintenance
        return if @closed || maintenance_running?
        # The driver passes this for servers it isn't connected to; like the stock pool, do no background I/O.
        return if @options[:populator_io] == false
        return unless (scheduler = Fiber.scheduler)

        @maintenance_stop = false

        # Parented to the scheduler rather than the current (e.g. request) task,
        # and transient so it doesn't keep the reactor alive.
        @maintenance_task = ::Async::Task.new(scheduler, transient: true, annotation: "#{self.class} maintenance (#{address})") do
          maintenance_loop
        end
        @maintenance_task.run
      end

      def stop_maintenance
        @maintenance_stop = true

        task = @maintenance_task
        if task&.alive? && owner_thread? && Fiber.scheduler
          task.stop
        else
          # Never stop a task from another thread; it sees the flag and exits.
          @maintenance_wakeup.signal
        end
      end

      def maintenance_loop
        until @closed || @maintenance_stop
          begin
            interrupt_stale_connections if @interrupt_pending
            retire_stale_idle if @retire_stale_pending
            @controller.retire_unviable

            # Keep going without waiting until min_size is reached.
            next if populate

            @maintenance_wakeup.wait(MAINTENANCE_INTERVAL)
          rescue StandardError => e
            log_warn("Connection pool maintenance failed for #{address}: #{e.class}: #{e.message}")
            @maintenance_wakeup.wait(MAINTENANCE_INTERVAL)
          end
        end

        if @retire_idle_pending
          @retire_idle_pending = false
          @controller.retire_idle(:pool_closed)
        end

        if @disconnect_in_use_pending
          @disconnect_in_use_pending = false
          disconnect_in_use(:pool_closed)
        end
      end

      # Disconnect in-use and pending connections from before the last clear, so fibers
      # blocked on a dead server wake up with a (retryable) PoolClearedError
      # instead of waiting for a socket timeout.
      def interrupt_stale_connections
        @interrupt_pending = false

        # In use or still being established, like the stock pool.
        stale = (@controller.in_use + @pending.to_a).select do |connection|
          connection.generation < @generation_manager.generation(service_id: connection.service_id)
        end
        return if stale.empty?

        stale.each { |connection| connection.disconnect!(reason: :stale, interrupted: true) }

        # Readers blocked in Kernel#select (the stock socket code) also watch the generation's pipe; closing it wakes them.
        stale.map { |connection| [connection.generation, connection.service_id] }.uniq.each do |stale_generation, service_id|
          @generation_manager.remove_pipe_fds(stale_generation, service_id: service_id)
        end
      end

      # Quacks like the stock pool's Populator, for driver compatibility.
      class PopulatorShim
        def initialize(pool)
          @pool = pool
        end

        def running?
          @pool.send(:maintenance_running?)
        end

        def run!
          @pool.send(:ensure_maintenance)
        end

        def stop!
          @pool.send(:stop_maintenance)
        end
      end
    end
  end
end
