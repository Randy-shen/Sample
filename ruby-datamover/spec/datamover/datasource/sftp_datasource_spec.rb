# frozen_string_literal: true

require 'spec_helper'
require 'net/ftp'
require 'fake_ftp'

describe SftpDatasource do
  let(:host) { '127.0.0.1' }
  let(:user) { 'user' }
  let(:password) { 'password' }
  let(:custom_port) { 21_212 }
  let(:filepath) { 'data.csv' }
  let(:local_filepath) { File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv') }

  let(:args) do
    {
      host: host,
      login: user,
      password: password,
      filepath: filepath
    }
  end

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

  let(:datasource) { described_class.new(args) }

  describe 'initialize' do
    it 'sets host' do
      expect(datasource.host).to eql(host)
    end

    it 'sets user from login' do
      expect(datasource.user).to eql(user)
    end

    it 'sets password' do
      expect(datasource.password).to eql(password)
    end

    it 'sets filepath' do
      expect(datasource.filepath).to eql(filepath)
    end

    it 'defaults port to 22' do
      expect(datasource.port).to eql(22)
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

    it 'defaults key_data to nil' do
      expect(datasource.key_data).to eql(nil)
    end

    it 'can set key_data' do
      args[:key_data] = 'some key data'

      datasource = described_class.new(args)

      expect(datasource.key_data).to eql(['some key data'])
    end

    it 'can set ssh_key' do
      args[:ssh_key] = 'some ssh key'

      datasource = described_class.new(args)

      expect(datasource.ssh_key).to eql(['some ssh key'])
    end

    it 'defaults ssh_key to nil' do
      expect(datasource.ssh_key).to eql(nil)
    end
  end

  describe '#logger' do
    it 'has a logger' do
      expect(datasource.logger).to_not eql(nil)
      expect(datasource.logger.class).to eql(Logger)
    end
  end

  describe '#stream_download' do
    it 'reads the SFTP socket stream to a CSV file' do
      args[:host] = 'test.rebex.net'
      args[:login] = 'demo'
      args[:password] = 'password'
      args[:filepath] = 'readme.txt'

      datasource = described_class.new(args)

      datasource.stream_download do |line|
        expect(line.class).to eql(Array)
      end
    end
  end

  describe '#stream_upload' do
    let(:sftp_client) { instance_double('sftp_client', file: File) }

    it 'implements stream_upload block' do
      filepath = File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv')
      input_datasource = FileDatasource.new(filepath: filepath)

      expect(input_datasource).to receive(:stream_download)

      args[:filepath] = '/tmp/data.csv'

      datasource = described_class.new(args)

      allow(datasource).to receive(:open_sftp_connection).and_yield(sftp_client)
      allow(datasource).to receive(:mkdir_p_on_directory!).and_return(true)

      datasource.stream_upload(input_datasource)
    end
  end
end
