# frozen_string_literal: true
require 'spec_helper'
require 'securerandom'
require 'tempfile'

describe S3Datasource do
  let(:access_key_id) { 'key' }
  let(:secret_access_key) { 'secret' }
  let(:region) { 'us-east-1' }
  let(:bucket_name) { 'v2_bucket' }

  let(:object_key) { 'somewhere.csv' }

  let(:params) do
    {
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      bucket_name: bucket_name
    }
  end

  let(:data) { 'some data' }

  let(:s3_object) do
    get = instance_double('s3_object_get', body: StringIO.new(data))
    put = true
    instance_double(SecureRandom.hex, key: object_key,
                                      exists?: true,
                                      content_length: 1_000,
                                      get: get,
                                      put: put)
  end

  let(:datasource) { described_class.new(params) }

  before do
    datasource.set_object_key(object_key)

    allow_any_instance_of(BufferedS3Writer::Client).to receive(:finish).and_return(true)
  end

  describe 'initialize' do
    it 'sets access_key_id' do
      expect(datasource.access_key_id).to eql(access_key_id)
    end

    it 'sets secret_access_key' do
      expect(datasource.secret_access_key).to eql(secret_access_key)
    end

    it 'sets region' do
      expect(datasource.region).to eql(region)
    end

    it 'sets bucket_name' do
      expect(datasource.bucket_name).to eql(bucket_name)
    end

    it 'defaults compress to false' do
      expect(datasource.compress).to eql(false)
    end

    it 'can set compress' do
      params[:compress] = true

      datasource = described_class.new(params)

      expect(datasource.compress).to eql(true)
    end

    it 'defaults decompress to false' do
      expect(datasource.decompress).to eql(false)
    end

    it 'can set decompress' do
      params[:decompress] = true

      datasource = described_class.new(params)

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
    it 'reads the S3 stream to a CSV file' do
      filepath = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv')
      body_stream = File.new(filepath)

      resp = instance_double('resp', body: body_stream)

      expect(datasource).to receive(:fetch_from_s3).and_return(resp)

      datasource.stream_download do |line|
        expect(line.class).to eql(Array)
      end
    end

    context 'when decompress is true' do
      it 'decompresses the CSV as it is read' do
        filepath = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv.gz')
        body_stream = File.new(filepath)

        resp = instance_double('resp', body: body_stream)

        params[:decompress] = true
        datasource = described_class.new(params)
        expect(datasource).to receive(:fetch_from_s3).and_return(resp)

        datasource.stream_download do |line|
          expect(line.class).to eql(Array)
        end
      end 
    end
  end

  describe '#stream_upload' do
    it 'implements stream_upload block' do
      allow(datasource).to receive(:filepath).and_return(Tempfile.new)

      input_datasource = instance_double('input_datasource', stream_download: [])

      expect(input_datasource).to receive(:stream_download)

      datasource.stream_upload(input_datasource)
    end
  end

  describe '#download' do
    it 'reads the entire body into memory' do
      allow(datasource).to receive(:s3_object).and_return(s3_object)

      expect(s3_object).to receive(:get)

      expect(datasource.download).to eql(data)
    end

    context 'when local_filename is passed as an option' do
      it 'sends data to the local file' do
        allow(datasource).to receive(:s3_object).and_return(s3_object)

        local_filename = Tempfile.new('somewhere')

        expect(s3_object).to receive(:get).with(response_target: local_filename)

        datasource.download(local_filename: local_filename)
      end
    end

    context 'when it does not exist' do
      it 'raises an error' do
        module DefaultDatasource
          class SourceIsEmpty < StandardError
          end
        end

        allow(datasource).to receive(:exists?).and_return(false)

        expect { datasource.download }.to raise_error(DefaultDatasource::SourceIsEmpty, 'SourceIsEmpty: no data returned')
      end
    end

    context 'when remote_directory is passed' do
      it 'changes the value of get_filepath' do
        original = datasource.get_filepath

        allow(datasource).to receive(:s3_object).and_return(s3_object)
        datasource.download(remote_directory: 'different/directory/')

        expect(datasource.get_filepath).to_not eql(original)
      end
    end

    context 'when filename is passed' do
      it 'changes the value of get_filepath' do
        original = datasource.get_filepath

        allow(datasource).to receive(:s3_object).and_return(s3_object)
        datasource.download(filename: 'different.csv')

        expect(datasource.get_filepath).to_not eql(original)
      end
    end
  end

  describe '#upload' do
    let(:buffer) { 'data to upload' }
    let(:options) do
      {
        buffer: buffer
      }
    end

    it 'uploads the buffer' do
      allow(datasource).to receive(:s3_object).and_return(s3_object)

      expect(s3_object).to receive(:put).with(body: buffer)

      expect(datasource.upload(options)).to eql(datasource)
    end

    context 'when remote_directory is passed' do
      it 'changes the value of get_filepath' do
        original = datasource.get_filepath

        allow(datasource).to receive(:s3_object).and_return(s3_object)
        datasource.upload(remote_directory: 'different/directory/')

        expect(datasource.get_filepath).to_not eql(original)
      end
    end

    context 'when filename is passed' do
      it 'changes the value of get_filepath' do
        original = datasource.get_filepath

        allow(datasource).to receive(:s3_object).and_return(s3_object)
        datasource.upload(filename: 'different.csv')

        expect(datasource.get_filepath).to_not eql(original)
      end
    end

    context 'when response_target is passed to options' do
      it 'changes the value of get_filepath' do
        original = datasource.get_filepath

        allow(datasource).to receive(:s3_object).and_return(s3_object)

        options[:response_target] = 'other.csv'
        datasource.upload(options)

        expect(datasource.get_filepath).to_not eql(original)
      end
    end

    context 'when local_filename is passed as an option' do
      it 'uploads contents of the local file' do
        allow(datasource).to receive(:s3_object).and_return(s3_object)

        local_filename = Tempfile.new('somewhere')
        local_filename.write('local data')

        expect(datasource).to receive(:upload_file_on_machine).with(local_filename)

        datasource.upload(local_filename: local_filename)
      end
    end
  end

  describe '#exists?' do
    it 'calls exists? against the instance of the s3 object' do
      expect(datasource).to receive(:s3_object).and_return(s3_object)

      expect(datasource.exists?).to eql(true)
    end
  end

  describe '#get_size' do
    it 'returns content length of s3 object' do
      allow(datasource).to receive(:s3_object).and_return(s3_object)

      expect(datasource.get_size).to eql(1_000)
    end

    context 'when s3 object does not exist' do
      it 'raises an error' do
        s3_object = instance_double('s3_object', exists?: false, content_length: 1_000)
        allow(datasource).to receive(:s3_object).and_return(s3_object)

        expect(datasource.instance_variable_get(:@logs)).to receive(:push).with('file does not exist')

        expect { datasource.get_size }.to raise_error(RuntimeError, 'file does not exist')
      end
    end
  end

  describe '#get_access_key_id' do
    it 'returns access_key' do
      expect(datasource.get_access_key_id).to eql(access_key_id)
    end
  end

  describe '#get_secret_access_key' do
    it 'returns secret_access_key' do
      expect(datasource.get_secret_access_key).to eql(secret_access_key)
    end
  end

  describe '#get_bucket_name' do
    it 'returns bucket_name' do
      expect(datasource.get_bucket_name).to eql(bucket_name)
    end
  end

  describe '#get_display_name' do
    it 'returns generated display name' do
      allow(datasource).to receive(:get_filepath).and_return(object_key)

      display_name = ['s3://', bucket_name, '/', object_key].join('')

      expect(datasource.get_display_name).to eql(display_name)
    end
  end

  describe '#get_object_key' do
    it 'returns value from filepath method' do
      expect(datasource.get_object_key).to eql(object_key)
    end
  end

  describe '#set_bucket_name' do
    it 'sets bucket name and returns self' do
      result = datasource.set_bucket_name('new_name')

      expect(result).to eql(datasource)
      expect(result.bucket_name).to eql('new_name')
    end
  end

  describe '#set_object_key' do
    it 'sets object key and returns self' do
      result = datasource.set_object_key('new_key')

      expect(result).to eql(datasource)
      expect(result.get_object_key).to eql('new_key')
    end
  end

  describe 'private' do
    describe '#buffered_s3_writer_client' do
      it 'initializes the S3 buffered writer' do
        expect(datasource.send(:buffered_s3_writer_client).class).to eql(BufferedS3Writer::Client)
      end
    end

    describe '#buffered_s3_writer_client_args' do
      it 'defaults compress to false' do
        expect(datasource.send(:buffered_s3_writer_client_args)[:compress]).to eql(false)
      end

      context 'when compress is true' do
        it 'can set compress to true' do
          params[:compress] = true

          datasource = described_class.new(params)

          expect(datasource.send(:buffered_s3_writer_client_args)[:compress]).to eql(true)
        end
      end
    end

    describe '#download_file_to_machine' do
      context 'when file object' do
        it 'calls s3_object.get' do
          file = Tempfile.new('somefile')
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(s3_object).to receive(:get).with(response_target: file)

          datasource.send(:download_file_to_machine, file)
        end
      end

      context 'when file path' do
        it 'calls File.open' do
          filepath = Tempfile.new('somefile').path
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(s3_object).to receive(:get).and_return(response_target: filepath)

          datasource.send(:download_file_to_machine, filepath)
        end
      end
    end

    describe '#upload_file_on_machine' do
      context 'when file object' do
        it 'calls File.open' do
          file = Tempfile.new('somefile')
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(File).to receive(:open).and_return(true)

          datasource.send(:upload_file_on_machine, file)
        end

        it 'calls s3 object.put' do
          file = Tempfile.new('somefile')
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(s3_object).to receive(:put).and_return(true)

          datasource.send(:upload_file_on_machine, file)
        end
      end

      context 'when file path' do
        it 'calls File.open' do
          filepath = Tempfile.new('somefile').path
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(File).to receive(:open).and_return(true)

          datasource.send(:upload_file_on_machine, filepath)
        end

        it 'calls s3 object.put' do
          filepath = Tempfile.new('somefile').path
          allow(datasource).to receive(:s3_object).and_return(s3_object)

          expect(s3_object).to receive(:put).and_return(true)

          datasource.send(:upload_file_on_machine, filepath)
        end
      end
    end

    describe '#s3_client' do
      it 'exists' do
        expect(datasource.send(:s3_client).class).to eql(Aws::S3::Client)
      end
    end

    describe '#credentials' do
      it 'exists' do
        expect(datasource.send(:credentials).class).to eql(Aws::Credentials)
      end
    end

    describe '#s3' do
      it 'exists' do
        expect(datasource.send(:s3).class).to eql(Aws::S3::Resource)
      end
    end

    describe '#s3_object' do
      it 'returns instance of an S3::Object' do
        expect(datasource.send(:s3_object).class).to eql(Aws::S3::Object)
      end

      it 'does not memoize' do
        original = datasource.send(:s3_object)

        second = datasource.send(:s3_object)

        expect(original).to_not eql(second)
      end
    end
  end
end
