# frozen_string_literal: true
require 'rubygems'
require 'bundler/setup'

# Must be currently set or requiring datamover/datasource fails
ENV['datamover_env'] = 'test'
ENV['datamover_datasource_path'] = 'spec/datasource.test.yml'

require 'byebug'
require 'datamover'
require 'support/helpers'
require 'rspec-benchmark'
require 'memory_profiler'
require 'timecop'

Dir[File.expand_path(File.join(File.dirname(__FILE__),'support','**','*.rb'))].each {|f| require f}

RSpec.configure do |config|
  config.include RSpec::Benchmark::Matchers

  config.before(:suite) { FakeS3Server.up }
  config.after(:suite) { FakeS3Server.down }
end
