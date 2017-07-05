# frozen_string_literal: true
require 'spec_helper'
require 'tempfile'

describe TC::Datamover do
  let(:options) do
    {
      logging: 1,
      verbose: 1,
      temp_path: './some/path',
      gpg_key_path: './some/gpg/key/path'
    }
  end

  let(:datamover_temp_path) { './some/path' }
  let(:datamover_gpg_key_path) { './some/gpg/key/path' }

  let(:lib) { TC::Datamover.new(options) }

  before do
    allow(ENV).to receive(:[]).and_call_original

    allow(ENV).to receive(:[]).with('datamover_temp_path').and_return(datamover_temp_path)
    allow(ENV).to receive(:[]).with('datamover_gpg_key_path').and_return(datamover_temp_path)
  end

  describe '.logger' do
    it 'sets a logger' do
      described_class.logger = nil

      expect(described_class.logger).to_not eql(nil)
      expect(described_class.logger.class).to eql(Logger)
    end

    context 'set your own logger' do
      it 'sets it' do
        custom_logger = double('custom_logger')

        described_class.logger = custom_logger

        expect(described_class.logger).to eql(custom_logger)

        described_class.logger = nil
      end
    end
  end

  describe 'initialize' do
    it 'sets @logging' do
      expect(lib.instance_variable_get(:@logging)).to eql(1)
    end

    it 'sets @verbose' do
      expect(lib.instance_variable_get(:@verbose)).to eql(1)
    end

    it 'sets @temp_path' do
      expect(lib.instance_variable_get(:@temp_path)).to eql(datamover_temp_path)
    end

    it 'sets @temp_path' do
      expect(lib.instance_variable_get(:@temp_path)).to eql(datamover_temp_path)
    end

    it 'defaults @datamover_logs to empty array' do
      expect(lib.instance_variable_get(:@datamover_logs)).to eql([])
    end
  end

  describe 'chained from/to' do
    context 'when file to puts' do
      it 'iterates through each_line of the stream given a real datasource' do
        filepath = File.join(File.dirname(__FILE__), 'files', 'data.csv')

        source = FileDatasource.new(filepath: filepath)
        target = PutsDatasource.new

        expect { lib.from_stream(source).to_stream(target) }.to output(File.read(filepath)).to_stdout
      end
    end

    context 'when file to file' do
      it 'iterates and stream writes to the new file' do
        filepath = File.join(File.dirname(__FILE__), 'files', 'data.csv')
        tempfile = Tempfile.new

        source = FileDatasource.new(filepath: filepath)
        target = FileDatasource.new(filepath: tempfile.path)

        expect(File.read(tempfile.path)).to_not eql(File.read(filepath))

        lib.from_stream(source).to_stream(target)

        expect(File.read(tempfile.path)).to eql(File.read(filepath))
      end
    end
  end

  describe '#from_stream' do
    let(:source) { instance_double('datasource', stream_download: []) }

    it 'sets stream_source and returns self' do
      expect(lib.stream_source).to eql(nil)

      expect(lib.from_stream(source)).to eql(lib)

      expect(lib.stream_source).to eql(source)
    end

    it 'optionally sets stream_delimiter and returns self' do
      delimiter = '\t'

      expect(lib.stream_delimiter).to eql(nil)

      expect(lib.from_stream(source, delimiter: delimiter)).to eql(lib)

      expect(lib.stream_delimiter).to eql(delimiter)
    end

    it 'does not yet support gpg_decrypt' do
      expect(lib.stream_gpg_decrypt).to eql(nil)

      expect(lib.from_stream(source, gpg_decrypt: true)).to eql(lib)

      expect(lib.stream_gpg_decrypt).to eql(false)
    end
  end

  describe '#transform' do
    let(:transformer) { instance_double('transformer', extract_format: [], load_format: []) }

    it 'sets transformer and returns self' do
      expect(lib.transformer.class).to eql(TC::Datamover::Transformer::Csv)

      expect(lib.transform(transformer)).to eql(lib)

      expect(lib.transformer).to eql(transformer)
    end
  end

  describe '#to_stream' do
    let(:target) do
      instance_double('datasource', stream_upload: true)
    end

    it 'sets stream_target and returns self' do
      expect(lib.stream_target).to eql(nil)

      expect(lib.to_stream(target)).to eql(lib)

      expect(lib.stream_target).to eql(target)
    end

    it 'calls upload_stream and returns self' do
      expect(target).to receive(:stream_upload)

      lib.to_stream(target)
    end
  end
end
