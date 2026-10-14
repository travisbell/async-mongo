# frozen_string_literal: true

require "async"
require "mongo"

require_relative "mongo/version"
require_relative "mongo/compatibility"
require_relative "mongo/connection"
require_relative "mongo/controller"
require_relative "mongo/connection_pool"
require_relative "mongo/server_extension"
require_relative "mongo/socket_extension"
require_relative "mongo/safe_finalizer"

module Async
  # Opt-in extensions for running the MongoDB Ruby driver under Async/Falcon.
  #
  # Requiring this file changes nothing. Call {Async::Mongo.install!} (or
  # require "async/mongo/install") to patch the driver.
  module Mongo
    class Error < StandardError; end

    # Raised by {install!} when the installed driver doesn't have the internals this gem relies on.
    class IncompatibleDriverError < Error; end

    @lock = Thread::Mutex.new
    @pool_installed = false
    @socket_installed = false

    class << self
      # Install the extensions into the MongoDB driver. Safe to call more than once.
      #
      # This is process-wide and can't be undone, so call it once at boot, before
      # any Mongo::Client connects.
      #
      # @parameter pool [Boolean] Use {ConnectionPool} for every (non load-balanced)
      #   server. Checkouts from fibers on the pool's reactor thread use the async
      #   pool; everything else (plain threads, other reactors) transparently uses
      #   the driver's stock pool.
      # @parameter socket [Boolean | Symbol] Route socket waits through the fiber
      #   scheduler ({SocketExtension}). `:auto` enables it when the Ruby/Async
      #   combination can interrupt fibers blocked on a socket that gets closed
      #   (Ruby 3.5+), which pool clearing relies on.
      # @returns [Module] self
      def install!(pool: true, socket: :auto)
        @lock.synchronize do
          install_pool! if pool

          if socket == true || (socket == :auto && Compatibility.interruptible_io?)
            install_socket!
          end
        end

        self
      end

      def pool_installed?
        @pool_installed
      end

      def socket_installed?
        @socket_installed
      end

      def installed?
        pool_installed? || socket_installed?
      end

      private

      def install_pool!
        return if @pool_installed

        Compatibility.check_pool!
        ::Mongo::Server.prepend(ServerExtension)
        ::Mongo::Server::ConnectionPool.singleton_class.prepend(SafeFinalizer)
        @pool_installed = true
      end

      def install_socket!
        return if @socket_installed

        Compatibility.check_socket!

        unless Compatibility.interruptible_io?
          warn("async-mongo: installing the socket extension on Ruby #{RUBY_VERSION}, which can't interrupt fibers blocked on a closed socket. Reads stuck on a dead server will only be woken by socketTimeoutMS.", uplevel: 2)
        end

        ::Mongo::Socket.prepend(SocketExtension)
        @socket_installed = true
      end
    end
  end
end
