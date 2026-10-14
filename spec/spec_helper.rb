# frozen_string_literal: true

require "async/mongo"
require "async/barrier"

Dir[File.join(__dir__, "support/**/*.rb")].each { |file| require file }

# The suite runs with the extensions installed; spec/async/mongo/install_spec.rb
# checks that merely requiring the gem changes nothing (in a subprocess).
Async::Mongo.install!

Mongo::Logger.logger.level = Logger::WARN

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand(config.seed)

  config.expect_with(:rspec) do |expectations|
    expectations.syntax = :expect
  end

  config.include(PoolHelpers)
end
