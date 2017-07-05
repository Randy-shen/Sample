# frozen_string_literal: true
require 'spec_helper'
require 'tempfile'

describe FileDatasource do
  let(:filepath) { File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv') }

  let(:args) do
    {
      filepath: filepath
    }
  end

  let!(:datasource) { described_class.new(args) }

  describe 'initialize' do
    it 'sets filepath' do
      expect(datasource.filepath).to eql(filepath)
    end

    it 'defaults decompress to false' do
      expect(datasource.decompress).to eql(false)
    end

    it 'can set decompress' do
      args[:decompress] = true

      datasource = described_class.new(args)

      expect(datasource.decompress).to eql(true)
    end
  end

  describe '#logger' do
    it 'has a logger' do
      expect(datasource.logger).to_not eql(nil)
      expect(datasource.logger.class).to eql(Logger)
    end
  end

  describe '#stream_download' do
    it 'calls file.each_line block' do
      file = datasource.send(:file)

      expect(file).to receive(:each_line)

      datasource.stream_download do |line|
        expect(line.class).to eql(Array)
      end
    end

    context 'when decompress is true' do
      it 'decompresses the CSV as it is read' do
        compressed_filepath = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv.gz')

        args[:filepath] = compressed_filepath
        args[:decompress] = true

        datasource = described_class.new(args)

        datasource.stream_download do |line|
          expect(line.class).to eql(Array)
        end
      end 
    end
  end

  describe '#stream_upload' do
    let(:tmp_dir) { File.join(File.dirname(__FILE__), '..', '..', '..', 'tmp') }

    before do
      FileUtils.rm(tmp_dir) rescue nil # cleanup
    end

    it 'implements stream_upload block' do
      allow(datasource).to receive(:filepath).and_return(Tempfile.new.path)

      input_datasource = instance_double('input_datasource', stream_download: [])

      expect(input_datasource).to receive(:stream_download)

      datasource.stream_upload(input_datasource)
    end

    context 'when filepath does not yet exist' do
      it 'smartly creates the file and implements stream_upload block' do
        tmp_filepath = File.join(tmp_dir, "data-#{SecureRandom.hex}.csv")

        allow(datasource).to receive(:filepath).and_return(tmp_filepath)

        input_datasource = instance_double('input_datasource', stream_download: [])

        expect(input_datasource).to receive(:stream_download)

        datasource.stream_upload(input_datasource)
      end
    end
  end

  describe 'private methods' do
    describe '#file' do
      it 'instantiates the filepath' do
        expect(datasource.send(:file).path).to eql(filepath)
      end

      it 'raises an error if filepath is invalid' do
        allow(datasource).to receive(:filepath).and_return('./no/exists.csv')

       expect { datasource.send(:file).path }.to raise_error(Errno::ENOENT)
      end
    end
  end
end
