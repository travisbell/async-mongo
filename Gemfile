# frozen_string_literal: true

source "https://rubygems.org"

gemspec

# CI tests against several driver releases, e.g. MONGO_RUBY_DRIVER_VERSION="~> 2.24.0".
gem "mongo", ENV.fetch("MONGO_RUBY_DRIVER_VERSION", ">= 2.24")

# mongo requires logger, which is no longer a default gem as of Ruby 4.0.
gem "logger"

group :development, :test do
  gem "rake"
  gem "rspec", "~> 3.13"
  gem "rubocop-shopify", require: false
end

group :benchmark do
  gem "async-http"
  gem "falcon"
  gem "puma"
  gem "rack"
  gem "rackup"
  gem "stackprof"
end
