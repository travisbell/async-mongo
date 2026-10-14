# frozen_string_literal: true

module Async
  module Mongo
    # Prepended to Mongo::Server by {Async::Mongo.install!} so that servers use
    # {ConnectionPool}. Load-balanced servers keep the stock pool, since they
    # pin connections to sessions and cursors.
    module ServerExtension
      # Same as the stock Mongo::Server#pool, except for the pool class.
      def pool
        return super if load_balancer?

        raise ::Mongo::Error::ServerNotUsable, address if unknown?

        @pool_lock.synchronize do
          opts = connected? ? options : options.merge(populator_io: false)
          @pool ||= ConnectionPool.new(self, opts).tap(&:ready)
        end
      end
    end
  end
end
