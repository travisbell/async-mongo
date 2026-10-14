# frozen_string_literal: true

require "socket"

RSpec.describe(Async::Mongo::SocketExtension) do
  # Prepended to a subclass, so this works whether or not install! enabled the extension on this Ruby.
  let(:socket_class) do
    Class.new(Mongo::Socket::TCP) { prepend Async::Mongo::SocketExtension }
  end

  let(:pair) { UNIXSocket.pair }
  let(:client_io) { pair[0] }
  let(:server_io) { pair[1] }

  after do
    pair.each { |io| io.close unless io.closed? }
  end

  def build_socket(io, timeout: nil)
    socket_class.allocate.tap do |socket|
      socket.instance_variable_set(:@socket, io)
      socket.instance_variable_set(:@timeout, timeout)
      socket.instance_variable_set(:@options, {})
      socket.instance_variable_set(:@host, "localhost")
      socket.instance_variable_set(:@port, 27017)
    end
  end

  def count_threads(&block)
    threads = 0
    TracePoint.new(:thread_begin) { threads += 1 }.enable(&block)
    threads
  end

  it "reads data that arrives in several chunks without spawning threads" do
    socket = build_socket(client_io)
    data = nil

    threads = count_threads do
      Sync do
        Async do
          3.times do |i|
            sleep 0.005
            server_io.write("chunk#{i}")
          end
        end

        data = socket.read(18)
      end
    end

    expect(data).to eq("chunk0chunk1chunk2")
    expect(data.encoding).to eq(Encoding::BINARY)
    expect(threads).to eq(0)
  end

  it "reads more than one buffer's worth" do
    socket = build_socket(client_io)
    payload = Random.bytes(300_000)

    Sync do
      Async { server_io.write(payload) }
      expect(socket.read(payload.bytesize)).to eq(payload)
    end
  end

  it "raises SocketTimeoutError when nothing arrives within the socket timeout" do
    socket = build_socket(client_io, timeout: 0.05)

    Sync do
      expect { socket.read(4) }.to raise_error(Mongo::Error::SocketTimeoutError)
    end
  end

  it "raises SocketError when the peer closes the connection" do
    socket = build_socket(client_io)

    Sync do
      Async do
        server_io.write("ab")
        server_io.close
      end

      expect { socket.read(4) }.to raise_error(Mongo::Error::SocketError)
    end
  end

  it "wakes a blocked reader with SocketError when another fiber closes the socket", if: Async::Mongo::Compatibility.interruptible_io? do
    socket = build_socket(client_io)

    Sync do
      Async do
        sleep 0.01
        client_io.close
      end

      started = monotonic_time
      expect { socket.read(4) }.to raise_error(Mongo::Error::SocketError)
      expect(monotonic_time - started).to be < 1
    end
  end

  it "waits for writability with a deadline" do
    socket = build_socket(client_io)

    Sync do
      expect(socket.send(:wait_for_socket_to_be_writable, monotonic_time + 1)).to be(true)
      expect(socket.send(:wait_for_socket_to_be_writable, monotonic_time - 1)).to be(false)
    end
  end

  it "uses the stock implementation outside a fiber scheduler" do
    socket = build_socket(client_io)
    server_io.write("data")

    expect(socket.read(4)).to eq("data")
  end
end
