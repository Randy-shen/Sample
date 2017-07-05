# frozen_string_literal: true

require 'spec_helper'
require 'net/ftp'
require 'fake_ftp'

describe FtpDatasource do
  let(:host) { '127.0.0.1' }
  let(:user) { 'user' }
  let(:pass) { 'password' }
  let(:custom_port) { 21_212 }
  let(:filepath) { 'data.csv' }
  let(:local_filepath) { File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv') }

  let(:args) do
    {
      host: host,
      user: user,
      pass: pass,
      filepath: filepath
    }
  end

  let(:datasource) { described_class.new(args) }

  before(:all) do
    @ftp_server = FakeFtp::Server.new(21_212, 21_213)

    @ftp_server.start

    filepath1 = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv')
    filepath2 = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv.gz')

    ftp = Net::FTP.new
    ftp.connect('127.0.0.1', 21_212)
    ftp.login('user', 'password')
    ftp.passive = true
    ftp.put(filepath1)
    ftp.put(filepath2)
    ftp.close
  end

  after(:all) do
    @ftp_server.stop
  end

  describe 'initialize' do
    it 'sets host' do
      expect(datasource.host).to eql(host)
    end

    it 'sets user' do
      expect(datasource.user).to eql(user)
    end

    it 'sets pass' do
      expect(datasource.pass).to eql(pass)
    end

    it 'sets filepath' do
      expect(datasource.filepath).to eql(filepath)
    end

    it 'defaults port to 21' do
      expect(datasource.port).to eql(21)
    end

    it 'can set port' do
      args[:port] = 21_212

      datasource = described_class.new(args)

      expect(datasource.port).to eql(21_212)
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
    it 'reads the FTP socket stream to a CSV file' do
      args[:port] = custom_port

      datasource = described_class.new(args)

      datasource.stream_download do |line|
        expect(line.class).to eql(Array)
      end
    end

    context 'when decompress is true' do
      it 'decompresses the CSV as it is read' do
        args[:decompress] = true
        args[:port] = custom_port
        args[:filepath] = 'data.csv.gz'

        datasource = described_class.new(args)

        datasource.stream_download do |line|
          expect(line.class).to eql(Array)
        end
      end
    end
  end

  describe '#stream_upload' do
    it 'implements stream_upload block' do
      filepath = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv')
      input_datasource = FileDatasource.new(filepath: filepath)

      expect(input_datasource).to receive(:stream_download)

      args[:port] = custom_port
      args[:filepath] = 'new_data.csv'

      datasource = described_class.new(args)

      datasource.stream_upload(input_datasource)

      # check it exists
      args[:port] = custom_port
      args[:filepath] = 'new_data.csv'
      datasource = described_class.new(args)
      datasource.stream_download do |line|
        puts line.inspect
      end
    end
  end
end
