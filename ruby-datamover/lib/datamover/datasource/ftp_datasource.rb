# frozen_string_literal: true

require 'open3'
require 'fastcsv'

require 'datamover/transformer/csv'
require 'datamover/datasource/shared/logger'

class FtpDatasource
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :host, :user, :pass, :filepath, :port, :decompress, :transformer

  def initialize(host:, user:, pass:, filepath:, port: 21, decompress: false, transformer: TC::Datamover::Transformer::Csv.new)
    self.host = host
    self.user = user
    self.pass = pass
    self.port = port
    self.filepath = filepath
    self.decompress = decompress
    self.transformer = transformer
  end

  def stream_download
    logger.info 'Starting read stream'

    Open3.popen2('curl', download_flags, ftp_url, '--user', ftp_user_auth) do |stdin, stdout, status_thread|
      stdout = Zlib::GzipReader.new(stdout) if decompress

      stdout.each_line do |line|
        transformer.extract_format(line) do |transformed_line|
          logger.debug transformed_line

          yield transformed_line
        end
      end

      raise 'Curl failed' unless status_thread.value.success?
    end

    logger.info 'Completed read stream'
  end

  def stream_upload(datasource)
    logger.info 'Starting write stream'

    Open3.popen2('curl', upload_flags, '-T', '-', ftp_url, '--user', ftp_user_auth) do |stdin, stdout, status_thread|
      datasource.stream_download do |line|
        transformer.load_format(line) do |transformed_line|
          logger.debug transformed_line

          stdin.puts transformed_line
        end
      end

      stdin.close
    end

    logger.info 'Completed write stream'
  end

  private

  def download_flags
    '-sfN' # silent, fail quietly, and No buffer
  end

  def upload_flags
    '-sfN' # silent, fail quietly, and No buffer
  end

  def ftp_url
    "ftp://#{host}:#{port}/#{filepath}"
  end

  def ftp_user_auth
    "#{user}:#{pass}"
  end
end
