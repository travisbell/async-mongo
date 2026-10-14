# frozen_string_literal: true

module Async
  module Mongo
    # Checks that the installed driver still has the internals async-mongo builds on.
    module Compatibility
      GENERATION_MANAGER_METHODS = [:generation, :pipe_fds, :remove_pipe_fds, :bump, :close_all_pipes].freeze
      READ_FROM_SOCKET_PARAMETERS = [[:req, :length], [:key, :socket_timeout], [:key, :csot]].freeze

      class << self
        # Whether closing a socket wakes a fiber blocked waiting on it. Needs
        # Ruby's Fiber::Scheduler#fiber_interrupt hook (Ruby 3.5+), implemented
        # by the Async scheduler.
        def interruptible_io?
          RUBY_VERSION >= "3.5" && ::Async::Scheduler.method_defined?(:fiber_interrupt)
        end

        def check_pool!
          missing = []
          missing << "Mongo::Server#pool" unless ::Mongo::Server.method_defined?(:pool)
          missing << "Mongo::Server#load_balancer?" unless ::Mongo::Server.method_defined?(:load_balancer?)
          missing << "Mongo::Server::ConnectionPool.finalize" unless ::Mongo::Server::ConnectionPool.respond_to?(:finalize)

          generation_manager = ::Mongo::Server::ConnectionPool::GenerationManager if defined?(::Mongo::Server::ConnectionPool::GenerationManager)
          if generation_manager
            GENERATION_MANAGER_METHODS.each do |method|
              missing << "GenerationManager##{method}" unless generation_manager.method_defined?(method)
            end
          else
            missing << "Mongo::Server::ConnectionPool::GenerationManager"
          end

          raise_incompatible!("connection pool", missing)
        end

        def check_socket!
          missing = []

          if ::Mongo::Socket.private_method_defined?(:read_from_socket)
            parameters = ::Mongo::Socket.instance_method(:read_from_socket).parameters
            missing << "Mongo::Socket#read_from_socket(length, socket_timeout:, csot:)" unless parameters == READ_FROM_SOCKET_PARAMETERS
          else
            missing << "Mongo::Socket#read_from_socket"
          end

          [:wait_for_socket_to_be_writable, :read_buffer_size, :allocate_string, :raise_timeout_error!].each do |method|
            missing << "Mongo::Socket##{method}" unless ::Mongo::Socket.private_method_defined?(method)
          end

          raise_incompatible!("socket", missing)
        end

        private

        def raise_incompatible!(feature, missing)
          return if missing.empty?

          raise IncompatibleDriverError, "async-mongo's #{feature} extension doesn't support mongo #{::Mongo::VERSION} (missing: #{missing.join(", ")})"
        end
      end
    end
  end
end
