# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'datamover/version'

Gem::Specification.new do |spec|
  spec.name          = 'datamover'
  spec.version       = Datamover::VERSION
  spec.authors       = ['dgu']
  spec.email         = ['dgu@truecar.com']

  spec.summary       = 'Data movement gem used within Truecar.'
  spec.description   = 'Gem to standardize data movement between different sources.'
  spec.homepage      = 'https://git.corp.tc/BI/ruby-datamover'
  spec.license       = 'MIT'

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = 'https://artifactory.corp.tc/artifactory/api/gems/ruby-gems'
  else
    raise 'RubyGems 2.0 or newer is required to protect against public gem pushes.'
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.12'
  spec.add_development_dependency 'down'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'dotenv'
  spec.add_development_dependency 'byebug'
  spec.add_development_dependency 'fakes3'
  spec.add_development_dependency 'fake_ftp'
  spec.add_development_dependency 'timecop'
  spec.add_development_dependency 'terminal-table'
  spec.add_development_dependency 'rspec-benchmark'
  spec.add_development_dependency 'memory_profiler'

  spec.add_runtime_dependency 'net-sftp', '~> 2.1', '>= 2.1.2'
  spec.add_runtime_dependency 'net-ssh', '~> 3.2'
  spec.add_runtime_dependency 'aws-sdk', '~> 2'
  spec.add_runtime_dependency 'pg', '~> 0.19.0'
  spec.add_runtime_dependency 'tiny_tds', '~> 1.0', '>= 1.0.4'
  spec.add_runtime_dependency 'activerecord', '>= 4.0', '< 6.0'
  spec.add_runtime_dependency 'gpgme', '~> 2.0.12'
  spec.add_runtime_dependency 'buffered_s3_writer', '~> 0.3'
  spec.add_runtime_dependency 'fastcsv', '~> 0.0.6'
end
