# frozen_string_literal: true

module Async
  module Mongo
    # A driver connection owned by a {ConnectionPool}. It's a subclass rather
    # than a patch, so connections belonging to the driver's own pools are
    # untouched.
    class Connection < ::Mongo::Server::Connection
      # @attribute [Float | Nil] Monotonic time of the last check-in (the stock #last_checkin is a Time).
      attr_reader :checked_in_at

      def checked_in!
        @checked_in_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        record_checkin!
        self
      end

      # Why the connection can't go back into the pool after being used, as a
      # ConnectionClosed reason, or nil if it can (same rules and order as the
      # stock pool's check-in).
      def close_reason
        return :stale if interrupted?
        return :error if error?

        pool = connection_pool
        return :pool_closed if pool.closed?
        # Already closed (e.g. after a network error); nothing more to report.
        return :stale if closed?
        # Pinned connections (load balancers) stay with their transaction or cursor until unpinned.
        return :stale if !pinned? && generation != pool.generation_manager.generation(service_id: service_id)

        nil
      end

      # Why an idle connection can't be handed out again, as a ConnectionClosed
      # reason, or nil if it can.
      def unviable_reason
        # Connections inherited across a fork share their socket with the parent.
        return :stale if pid != Process.pid
        return :stale unless socket

        reason = close_reason
        return reason if reason

        max_idle_time = connection_pool.max_idle_time
        return :idle if max_idle_time && @checked_in_at && Process.clock_gettime(Process::CLOCK_MONOTONIC) - @checked_in_at > max_idle_time

        nil
      end

      def reusable?
        close_reason.nil?
      end

      def viable?
        unviable_reason.nil?
      end
    end
  end
end
