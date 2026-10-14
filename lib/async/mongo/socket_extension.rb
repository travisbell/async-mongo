# frozen_string_literal: true

require "io/wait"

module Async
  module Mongo
    # Prepended to Mongo::Socket by {Async::Mongo.install!}.
    #
    # Mongo::Socket waits for its socket with Kernel.select, which under a
    # fiber scheduler goes through Scheduler#io_select. Async implements that by
    # spawning a thread per call, and the driver hits it on nearly every read
    # (the reply is rarely there yet when read_nonblock is first tried).
    #
    # When a fiber scheduler is active, this waits with IO#wait_readable /
    # IO#wait_writable instead, which the scheduler handles in its event loop.
    # Otherwise (e.g. the driver's monitor threads) it falls back to the stock
    # code.
    #
    # The stock read also selects on the pool generation's pipe, so that
    # clearing a pool can wake blocked readers. That isn't needed here: on Ruby
    # 3.5+, closing the socket raises IOError in a fiber blocked in
    # wait_readable, which the driver maps to a SocketError.
    #
    # Connection establishment (TCP/TLS connect) still uses IO.select. It only
    # happens when a pool opens a new connection, so it isn't worth overriding.
    module SocketExtension
      private

      def read_from_socket(length, socket_timeout: nil, csot: false)
        return super unless Fiber.scheduler
        return allocate_string(0) if length == 0

        _timeout = socket_timeout || timeout
        deadline = nil
        if _timeout
          if _timeout > 0
            deadline = ::Mongo::Utils.monotonic_time + _timeout
          elsif _timeout < 0
            raise_timeout_error!("Negative timeout #{_timeout} given to socket", csot)
          end
        end

        buf_size = read_buffer_size
        buf_size = length if length < buf_size

        buf = allocate_string(buf_size)
        data = nil
        retrieved = 0

        while retrieved < length
          retrieve = length - retrieved
          retrieve = buf_size if retrieve > buf_size

          # Reading from a TLS socket may need to write first, so it can ask to wait for either.
          case (chunk = @socket.read_nonblock(retrieve, buf, exception: false))
          when :wait_readable, :wait_writable
            wait_for_socket(chunk, deadline, _timeout, csot)
            next
          when nil
            raise EOFError, "end of file reached"
          end

          # Read everything in one go: return the buffer as is, saving an allocation and a copy.
          return chunk if retrieved == 0 && chunk.length == length

          data = allocate_string(length) if data.nil?
          data[retrieved, chunk.length] = chunk
          retrieved += chunk.length
        end

        data
      end

      def wait_for_socket_to_be_writable(deadline)
        return super unless Fiber.scheduler

        wait_timeout = deadline - ::Mongo::Utils.monotonic_time
        wait_timeout > 0 && !@socket.to_io.wait_writable(wait_timeout).nil?
      end

      def wait_for_socket(event, deadline, timeout, csot)
        wait_timeout = nil

        if deadline
          wait_timeout = deadline - ::Mongo::Utils.monotonic_time
          raise_timeout_error!("Took more than #{timeout} seconds to receive data", csot) if wait_timeout <= 0
        end

        # An SSLSocket isn't an IO, so wait on the underlying TCP socket.
        return if @socket.to_io.public_send(event, wait_timeout)

        raise_timeout_error!("Took more than #{timeout} seconds to receive data (wait timed out)", csot)
      end
    end
  end
end
