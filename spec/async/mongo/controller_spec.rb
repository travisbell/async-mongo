# frozen_string_literal: true

RSpec.describe(Async::Mongo::Controller) do
  let(:resource_class) do
    Struct.new(:id, :reusable, :viable, :closed_reason) do
      def close_reason
        reusable ? nil : :stale
      end

      def unviable_reason
        viable ? nil : :stale
      end

      def disconnect!(reason:)
        self.closed_reason = reason
      end
    end
  end

  let(:created) { [] }

  def build_controller(limit: 3, max_connecting: 2, wait_timeout: 1, &before_create)
    Async::Mongo::Controller.new(limit: limit, max_connecting: max_connecting, wait_timeout: wait_timeout, address: "test") do |_context|
      before_create&.call
      resource_class.new(created.size + 1, true, true).tap { |resource| created << resource }
    end
  end

  it "creates connections up to the limit, then times out" do
    controller = build_controller(limit: 2, wait_timeout: 0.05)

    Sync do
      a = controller.acquire
      b = controller.acquire
      expect([a, b].map(&:id)).to eq([1, 2])
      expect(controller.size).to eq(2)

      expect { controller.acquire }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout, /2 in use/)
    end
  end

  it "reuses the most recently released connection first" do
    controller = build_controller

    Sync do
      a = controller.acquire
      b = controller.acquire
      controller.release(a)
      controller.release(b)

      expect(controller.acquire).to equal(b)
      expect(controller.acquire).to equal(a)
    end
  end

  it "retires connections that are no longer reusable on release" do
    controller = build_controller

    Sync do
      a = controller.acquire
      a.reusable = false
      controller.release(a)

      expect(a.closed_reason).to eq(:stale)
      expect(controller.size).to eq(0)
    end
  end

  it "retires idle connections that are no longer viable instead of handing them out" do
    controller = build_controller

    Sync do
      a = controller.acquire
      controller.release(a)
      a.viable = false

      b = controller.acquire
      expect(b).not_to equal(a)
      expect(a.closed_reason).to eq(:stale)
    end
  end

  it "raises the error once set, waking waiters" do
    controller = build_controller(limit: 1, wait_timeout: 5)

    Sync do
      controller.acquire
      error = nil

      waiter = Async do
        controller.acquire
      rescue Mongo::Error => e
        error = e
      end

      sleep 0.01
      controller.error = Mongo::Error::PoolClosedError.new("test", nil)
      controller.signal
      waiter.wait

      expect(error).to be_a(Mongo::Error::PoolClosedError)
    end
  end

  it "establishes at most max_connecting connections at once, without exceeding the limit" do
    connecting = 0
    peak = 0

    controller = build_controller(limit: 3, max_connecting: 2) do
      connecting += 1
      peak = [peak, connecting].max
      sleep 0.01
      connecting -= 1
    end

    Sync do
      barrier = Async::Barrier.new
      sizes = []

      8.times do
        barrier.async do
          connection = controller.acquire
          sizes << controller.size
          sleep 0.005
          controller.release(connection)
        end
      end

      barrier.wait

      expect(peak).to eq(2)
      expect(sizes.max).to be <= 3
      expect(controller.size).to eq(3)
    end
  end

  it "hands out idle connections while another connection is being established" do
    slow = false
    controller = build_controller { sleep 0.2 if slow }

    Sync do
      idle = controller.acquire
      slow = true

      creating = Async { controller.acquire }
      sleep 0.01
      controller.release(idle)

      started = monotonic_time
      expect(controller.acquire).to equal(idle)
      expect(monotonic_time - started).to be < 0.05

      creating.wait
    end
  end

  it "frees the connecting slot when establishing a connection fails" do
    fail_next = true
    controller = build_controller(max_connecting: 1) do
      if fail_next
        fail_next = false
        raise Mongo::Error::SocketError, "refused"
      end
    end

    Sync do
      expect { controller.acquire }.to raise_error(Mongo::Error::SocketError)
      expect(controller.connecting_count).to eq(0)
      expect(controller.acquire.id).to eq(1)
    end
  end

  describe "waiting" do
    it "serves waiters first come, first served" do
      controller = build_controller(limit: 1, wait_timeout: 5)

      Sync do
        held = controller.acquire
        order = []

        tasks = 5.times.map do |i|
          Async do
            connection = controller.acquire
            order << i
            controller.release(connection)
          end.tap { sleep 0.001 }
        end

        controller.release(held)
        tasks.each(&:wait)

        expect(order).to eq([0, 1, 2, 3, 4])
      end
    end

    it "doesn't let new requests barge ahead of waiting ones" do
      controller = build_controller(limit: 1, wait_timeout: 5)

      Sync do
        held = controller.acquire
        waiter = Async { controller.acquire }
        sleep 0.001
        expect(controller.waiting_count).to eq(1)

        controller.release(held)
        # The released connection went to the waiter, so a newcomer has to queue.
        expect { controller.acquire(timeout: 0.01) }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout, /1 in use/)
        expect(waiter.wait).to equal(held)
      end
    end

    it "wakes only the waiter being served" do
      controller = build_controller(limit: 1, wait_timeout: 5)

      Sync do
        held = controller.acquire
        woken = []
        tasks = 3.times.map { |i| Async { controller.acquire.tap { woken << i } } }
        sleep 0.001

        controller.release(held)
        sleep 0.001
        expect(woken).to eq([0])
        expect(controller.waiting_count).to eq(2)

        tasks.first.wait
        tasks.drop(1).each(&:stop)
      end
    end

    it "grants connecting slots to waiters when connections are retired" do
      controller = build_controller(limit: 1, wait_timeout: 5)

      Sync do
        held = controller.acquire
        waiter = Async { controller.acquire }
        sleep 0.001

        held.reusable = false
        controller.release(held)

        expect(waiter.wait.id).to eq(2)
      end
    end

    it "passes a grant on when the waiter is cancelled" do
      controller = build_controller(limit: 1, wait_timeout: 5)

      Sync do
        held = controller.acquire
        first = Async { controller.acquire }
        second = Async { controller.acquire }
        sleep 0.001

        first.stop
        controller.release(held)

        expect(second.wait).to equal(held)
        expect(controller.waiting_count).to eq(0)
      end
    end

    it "leaves the line on timeout" do
      controller = build_controller(limit: 1, wait_timeout: 0.01)

      Sync do
        controller.acquire
        expect { controller.acquire }.to raise_error(Mongo::Error::ConnectionCheckOutTimeout)
        expect(controller.waiting_count).to eq(0)
      end
    end
  end

  describe "#take_peak_in_use" do
    it "returns the most connections in use since the last call" do
      controller = build_controller

      Sync do
        expect(controller.take_peak_in_use).to eq(0)

        a = controller.acquire
        b = controller.acquire
        controller.release(a)
        controller.release(b)
        controller.acquire

        expect(controller.take_peak_in_use).to eq(2)
        expect(controller.take_peak_in_use).to eq(1)
      end
    end

    it "only tracks once asked" do
      controller = build_controller

      Sync do
        controller.release(controller.acquire)

        expect(controller.take_peak_in_use).to eq(0)
      end
    end

    it "never exceeds the limit" do
      controller = build_controller(limit: 2, wait_timeout: 1)
      controller.take_peak_in_use

      Sync do |task|
        tasks = 6.times.map do
          task.async do
            connection = controller.acquire
            sleep(0.001)
            controller.release(connection)
          end
        end
        tasks.each(&:wait)
      end

      expect(controller.take_peak_in_use).to eq(2)
    end
  end

  describe "#create_idle" do
    it "creates idle connections up to the given maximum" do
      controller = build_controller(limit: 5)

      Sync do
        3.times { controller.create_idle(2) }

        expect(controller.idle_count).to eq(2)
        expect(controller.in_use_count).to eq(0)
      end
    end
  end

  describe "#retire_idle" do
    it "retires the idle connections selected by the block" do
      controller = build_controller

      Sync do
        a = controller.acquire
        b = controller.acquire
        controller.release(a)
        controller.release(b)

        expect(controller.retire_idle { |connection| :idle if connection.equal?(a) }).to eq(1)
        expect(a.closed_reason).to eq(:idle)
        expect(controller.idle_count).to eq(1)
      end
    end
  end
end
