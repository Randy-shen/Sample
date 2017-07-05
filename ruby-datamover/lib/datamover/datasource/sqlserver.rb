require 'csv'

require 'tiny_tds'

require 'datamover/datasource/default.rb'
require 'datamover/datasource/shared_helpers.rb'
require 'datamover/datasource/shared/logger'

class SqlServerDatasource
  include DefaultDatasource
  include SharedHelpers
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :host, :username, :password, :database_name, :row_count

  def initialize(host:, login:, password:, **options)
    self.host = host
    self.username = login
    self.password = password
    self.database_name = options[:database_name] || options[:database]

    self.row_count = 0
    @logs = []
  end

  def stream_download(&block)
    logger.info 'Starting read stream'

    tds_result.each(as: :array, cache_rows: false) do |record|
      logger.debug clean_invisible_characters(record)

      yield clean_invisible_characters(record) # already formatted as an array
    end

    logger.info 'Completed read stream'
  end

  def stream_upload(datasource)
    msg = 'does not implement stream_upload'

    logger.fatal msg

    raise NotImplementedError, msg
  end

  def database(database_name)
    self.database_name = database_name

    unmemoize_conn
    self
  end

  def logoff
    conn.close if conn && conn.active?
  end

  def set_query(command)
    @sql_command = command
    self
  end

  def get_display_name
    if get_filepath.to_s.empty?
      "#{host}/#{database_name}/#{@schema}.#{@table}"
    else
      "#{host}/#{database_name}/#{get_filepath}"
    end
  end

  def query(sql_command: @sql_command, **options)
    log(sql_command, options[:verbose])

    result = conn.execute(sql_command)

    return result
  end

  def exists?(sql_command = @sql_command)
    sql_exists = "SELECT CASE WHEN EXISTS(#{sql_command}) THEN 't' ELSE 'f' END as [exists]  "
    result = query(sql_command: sql_exists)

    if result[0]['exists'].eql? "t"
      return true
    else
      return false
    end
  end

  def download(sql_command: @sql_command, **options)
    local_filename = options[:local_filename]

    result = query(sql_command: sql_command, **options)
    
    if local_filename
      CSV.open(local_filename,"wb") do |csv|
        result.each(:as => :array, :cache_rows => false) do |record|
          self.row_count = row_count.next

          csv << record.map do |column|
            column.class == String ? column.gsub(/\r?\n/, ' ') : column
          end
        end
      end
    else
      buffer = CSV.generate do |csv|
        result.each(:as => :array, :cache_rows => false) do |record|
          self.row_count = row_count.next

          csv << record.map do |column|
            column.class == String ? column.gsub(/\r?\n/, ' ') : column
          end
        end
      end

      return buffer
    end
  end

  # <b>DEPRECATED:</b> Do not use.
  def login
    warn '[DEPRECATION] `login` is deprecated. Do not use. The gem datamover now handles login on its own - lazily instantiating for improved performance.'
    conn
    self
  end

  private

  def conn
    @conn ||= TinyTds::Client.new(conn_payload)
  end

  def unmemoize_conn
    @conn = nil
  end

  def conn_payload
    {
      host: host,
      username: username,
      password: password,
      timeout: 0,
      database: database_name
    }.delete_if {|k, v| v.nil?}
  end

  def tds_result
    @tds_result ||= query(sql_command: @sql_command)
  end

  def clean_invisible_characters(record)
    record.map! do |value|
      value.gsub!(/\r?\n/, ' '.freeze) if value.class == String

      value
    end
  end
end
