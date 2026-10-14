# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "rubocop/rake_task"

RSpec::Core::RakeTask.new(:spec)
RuboCop::RakeTask.new

desc "Run the Puma vs Falcon benchmark (see bench/README.md)"
task :bench do
  ruby "bench/run.rb"
end

task default: [:spec, :rubocop]
