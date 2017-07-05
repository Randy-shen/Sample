require 'net/ssh'
require 'net/sftp'

require 'datamover/transformer/csv'
require 'datamover/datasource/default.rb'
require 'datamover/datasource/shared/logger'

class SftpDatasource
  include DefaultDatasource
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :host, :user, :password, :port, :filepath, :port, :decompress,
                :key_data, :ssh_key, :transformer

  def initialize(host:, login:, password: nil, port: 22, filepath: nil,
                 decompress: false, key_data: nil, ssh_key: nil,
                 transformer: TC::Datamover::Transformer::Csv.new)
    self.host = host
    self.user = login
    self.password = password
    self.port = port
    self.filepath = filepath
    self.decompress = decompress
    self.key_data = key_data.to_s.empty? ? nil : [key_data]
    self.ssh_key = ssh_key.to_s.empty? ? nil : [ssh_key]
    self.transformer = transformer

    @logs = []
  end

  def stream_download
    logger.info 'Starting read stream'

    open_sftp_connection do |sftp_client|
      sftp_client.file.open(filepath, 'r') do |file|
        while
          line = file.gets

          break if file.eof?

          transformer.extract_format(line) do |transformed_line|
            logger.debug transformed_line

            yield transformed_line
          end
        end
      end
    end

    logger.info 'Completed read stream'
  end

  def stream_upload(datasource)
    logger.info 'Starting write stream'

    open_sftp_connection do |sftp_client|
      mkdir_p_on_directory!(sftp_client)

      sftp_client.file.open(filepath, 'w') do |file|
        datasource.stream_download do |line|
          logger.debug upload_format(line)

          file.puts upload_format(line)
        end
      end
    end

    logger.info 'Completed write stream'
  end

  def download(remote_directory: self.get_remote_directory, filename: self.get_filename, **options)
    local_filename = options[:local_filename]
    remote_filename = File.join(remote_directory,filename)

    if self.exists?(remote_directory:remote_directory, filename:filename)
      if local_filename
        if remote_filename.end_with?('/')
          if !local_filename.end_with?('/')
            local_filename << '/'
          end

          dls = old_sftp_client.dir.entries(remote_directory).map{|e| e.name}.map{|f| ssh.sftp.download("#{remote_directory}#{f}","#{local_filename}#{f}")}
          dls.each{|d| d.wait}
        else
          old_sftp_client.download!(remote_filename,local_filename)
        end
      else
        old_sftp_client.download!(remote_filename)
      end
    else
      raise SourceIsEmpty,'No data returned.'
    end
  end

  def upload(remote_directory: self.get_remote_directory, filename: self.get_filename, **options)
    local_filename = options[:local_filename]
    buffer = options[:buffer]
    response_target = options[:response_target]

    self.set_remote_directory(remote_directory)

    if response_target
      self.set_filename(response_target)
    else
      self.set_filename(filename)
    end

    remote_filename = File.join(self.get_remote_directory,self.get_filename)

    data = local_filename ? File.open(local_filename,'rb') : buffer

    old_sftp_client.file.open(remote_filename,"w") do |f|
      f.puts data
    end

    self
  end

  def get_list(remote_directory = @remote_directory)
    files_h = {}

    old_sftp_client.dir.entries(remote_directory)
        .map{|f| ["name", "size", "atime", "mtime"].zip([f.name, f.attributes::size, f.attributes::atime, f.attributes::mtime])}
        .each{|f| files_h[f[0][1]] = f.to_h} #f[0][1] extracts the filename out of the array to become the key of the hash

    files_h
  end

  def exists?(remote_directory: self.get_remote_directory, filename: self.get_filename)
    old_sftp_client.dir.entries(remote_directory).map{|e| e.name}.include? filename
  end

  def get_display_name
    "//#{host}/#{self.get_filepath}"
  end

  private

  def sftp_url
    "sftp://#{host}:#{port}/#{filepath}"
  end

  def download_flags
    '-N' # silent, fail quietly, and No buffer
  end

  def sftp_user_auth
    "#{user}:#{password}"
  end

  def conn_options
    {
      password: password,
      port: port,
      keys: ssh_key,
      key_data: key_data
    }.delete_if { |_, v| blank?(v) }
  end

  def filepath_dir
    File.dirname(filepath)
  end

  def filepath_dir_parts
    Pathname.new(filepath_dir).each_filename.to_a
  end

  def mkdir_p_on_directory!(sftp_client)
    logger.info "Creating filepath directory at: #{filepath_dir}"

    parts = filepath_dir_parts.reverse

    growing_path = []

    while parts.length > 0
      growing_path.push(parts.pop)

      create_directory_if_it_does_not_exist!(sftp_client, File.join(growing_path))
    end
  end

  def create_directory_if_it_does_not_exist!(sftp_client, dir)
    logger.info "Creating directory at: #{dir}"

    sftp_client.mkdir!(dir)
  rescue Net::SFTP::StatusException => e
    raise unless carry_on_error_codes.include?(e.code)
    # directory already exists. carry on.
    logger.info 'Filepath directory already exists.'
  end

  def carry_on_error_codes
    [4, 11]
  end

  def open_sftp_connection
    Net::SFTP.start(host, user, conn_options) do |sftp_client|
      yield sftp_client
    end
  end
  
  def blank?(value)
    value.respond_to?(:empty?) ? value.empty? : !value
  end

  def old_sftp_client
    @sftp_client ||= fetch_old_sftp_client
  end

  def fetch_old_sftp_client
    if key_data.any?
      Net::SSH.start(host, user, key_data: key_data).sftp
    elsif ssh_key.any?
      Net::SSH.start(host, user, keys: ssh_key).sftp
    elsif password
      Net::SFTP.start(host, user, password: password)
    else
      msg = 'No authentication provided for SFTP login!'

      @logs.push(msg)

      raise msg
    end
  end
end
