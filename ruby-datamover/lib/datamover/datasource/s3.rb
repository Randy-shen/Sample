require 'zlib'
require 'aws-sdk'
require 'buffered_s3_writer'

require 'datamover/transformer/csv'
require 'datamover/datasource/default.rb'
require 'datamover/datasource/shared/logger'

class S3Datasource
  include DefaultDatasource
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :access_key_id, :secret_access_key, :region, :bucket_name,
                :compress, :decompress, :transformer

  def initialize(access_key_id:, secret_access_key:, region:, bucket_name:,
                 compress: false, decompress: false,
                 transformer: TC::Datamover::Transformer::Csv.new)
    self.access_key_id = access_key_id
    self.secret_access_key = secret_access_key
    self.region = region
    self.bucket_name = bucket_name
    self.compress = compress
    self.decompress = decompress
    self.transformer = transformer

    @logs = []
  end

  def stream_download(&block)
    logger.info 'Requesting S3 file'

    resp = fetch_from_s3

    logger.info 'Starting read stream'

    reader = resp.body # resp.body is a StringIO
    reader = Zlib::GzipReader.new(resp.body) if decompress

    reader.each_line do |line|
      transformer.extract_format(line) do |transformed_line|
        logger.debug transformed_line

        yield transformed_line
      end
    end

    reader.close

    logger.info 'Completed read stream'
  end

  def stream_upload(datasource)
    logger.info 'Starting write stream'

    datasource.stream_download do |line|
      transformer.load_format(line) do |transformed_line|
        logger.debug transformed_line

        buffered_s3_writer_client.push transformed_line
      end
    end

    buffered_s3_writer_client.finish

    logger.info 'Completed write stream'
  end

  def download(remote_directory: get_remote_directory, filename: get_filename, **options)
    local_filename = options[:local_filename]

    set_remote_directory(remote_directory)
    set_filename(filename)

    if exists?
      if local_filename
        local_filename << s3_object.key if local_filename.path.end_with?('/') # I don't think this is even possible to ever occur on unix machines.

        download_file_to_machine(local_filename)
      else
        s3_object.get.body.read # WARNING: reads the entire file into memory. Be wary of using this for large files at any decent scale.
      end
    else
      msg = 'SourceIsEmpty: no data returned'

      @logs.push(msg)

      raise SourceIsEmpty, msg
    end
  end

  def upload(remote_directory: get_remote_directory, filename: get_filename, **options)
    local_filename = options[:local_filename]
    buffer = options[:buffer]
    response_target = options[:response_target]

    set_remote_directory(remote_directory)
    set_filename(filename)
    set_filename(response_target) if response_target

    if local_filename
      upload_file_on_machine(local_filename)
    else
      s3_object.put(body: buffer)
    end

    self
  end

  def get_size
    if s3_object.exists?
      s3_object.content_length
    else
      msg = 'file does not exist'

      @logs.push(msg)

      raise msg
    end
  end

  def exists?
    s3_object.exists?
  end

  def set_bucket_name(bucket_name)
    tap { |i| i.bucket_name = bucket_name.to_s }
  end

  def set_object_key(object_key)
    tap { set_filepath(object_key) }
  end

  def get_access_key_id
    access_key_id.to_s
  end

  def get_secret_access_key
    secret_access_key.to_s
  end

  def get_bucket_name
    bucket_name.to_s
  end

  def get_display_name
    "s3://#{bucket_name}/#{get_object_key}"
  end

  def get_object_key
    get_filepath
  end

  private

  def buffered_s3_writer_client
    @buffered_s3_writer_client ||= BufferedS3Writer::Client.new(buffered_s3_writer_client_args)
  end

  def buffered_s3_writer_client_args
    {
      key: get_object_key,
      bucket_name: bucket_name,
      region: region,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      compress: compress
    }
  end

  def s3_client
    @s3_client ||= Aws::S3::Client.new(s3_client_options)
  end

  def s3_client_options
    options = {
      region: region,
      credentials: credentials
    }

    options
  end

  def credentials
    Aws::Credentials.new(access_key_id, secret_access_key)
  end

  def s3
    @s3 ||= Aws::S3::Resource.new(client: s3_client)
  end

  def s3_object
    s3.bucket(get_bucket_name).object(get_object_key)
  end

  def upload_file_on_machine(file_or_filepath)
    File.open(file_or_filepath, 'rb') do |file|
      s3_object.put(body: file)
    end
  end

  def download_file_to_machine(file_or_filepath)
    s3_object.get(response_target: file_or_filepath) # writes the entire file to disk.
  end

  def fetch_from_s3
    s3_client.get_object(bucket: bucket_name, key: get_object_key)
  end
end
