# frozen_string_literal: true
require 'csv'
require 'pg'

require 'datamover/datasource/default.rb'
require 'datamover/datasource/shared_helpers.rb'
require 'datamover/datasource/shared/logger'

class PgDatasource
  include DefaultDatasource
  include SharedHelpers
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :host, :port, :user, :password, :database_name

  attr_reader :row_count, :rows_deleted, :rows_inserted

  def initialize(host:, port:, login:, password:, **options)
    self.host = host
    self.port = port
    self.user = login
    self.password = password
    self.database_name = options[:database_name]
    @logs = []
  end

  def stream_download
    msg = 'does not yet implement stream_download'

    logger.fatal msg

    raise NotImplementedError, msg 
  end

  def stream_upload(datasource)
    validate_load_option!(@load_options)

    stream_copy_to_staging_table(datasource)

    case @load_options
    when 'insert'
      insert_from_staging_table
    when 'upsert'
      contrived_upsert_from_staging_table
    when 'bulk_delete_append'
      bulk_delete_and_insert_from_staging_table
    when 'truncate'
      drop_main_table_and_rename_staging_table_to_main_table
    end
  ensure
    query(sql_command: drop_staging_table_command(@schema, @table)) # cleanup staging table
  end

  def database(database_name)
    self.database_name = database_name

    unmemoize_conn
    self
  end

  def logoff
    conn.close if conn
  end

  def set_query(command)
    @sql_command = command
    self
  end

  def get_display_name
    if self.get_filepath.to_s.empty?
      "#{host}/#{database_name}/#{@schema}.#{@table}"
    else
      "#{host}/#{database_name}/#{self.get_filepath}"
    end
  end

  def query(sql_command: @sql_command, **options)
    log(sql_command, options[:verbose])

    result = conn.exec(sql_command)

    return result
  end

  def exists?(sql_command = @sql_command)
    sql_exists = "SELECT exists(#{sql_command})"
    result = self.query(sql_command:sql_exists)

    if result[0]['exists'].eql? "t"
      return true
    else
      return false
    end
  end

  def download(sql_command: @sql_command, **options)
    local_filename = options[:local_filename]

    result = self.query(sql_command:sql_command, **options)

    if local_filename
      CSV.open(local_filename,"wb") do |csv|
        result.each do |record|
          csv << record.collect {|key,value| value}
        end
      end
    else
      buffer = CSV.generate do |csv|
        result.each do |record|
          csv << record.collect {|key,value| value}
        end
      end

      return buffer
    end
  end

  def upload(file:, schema: @schema, table: @table, load_options: @load_options, primary_key: @primary_key, bulk_delete_key_value: @bulk_delete_key_value, **options)
    validate_load_option!(load_options)

    create_query = create_staging_table_command(schema, table)
    drop_staging_query = drop_staging_table_command(schema, table)
    drop_main_query = drop_main_table_command(schema, table)
    rename_query = rename_table_staging_to_main_command(schema, table)
    insert_query = insert_from_staging_table_command(schema, table)
    analyze_query = analyze_table_command(schema, table)
    delete_from_main_query = delete_from_main_table_command(primary_key, schema, table)
    bulk_delete_from_main_query = bulk_delete_from_main_table_command(bulk_delete_key_value, schema, table)

    query(sql_command: create_query, **options)

    copy(file: file, schema: schema, table: staging_table(table), **options)

    if (load_options == 'insert')
      @rows_deleted = 0
      result = query(sql_command: insert_query, **options)
      @rows_inserted = result.cmd_tuples
    elsif (load_options == 'upsert')
      result = query(sql_command: delete_from_main_query, **options)
      @rows_deleted = result.cmd_tuples
      result = query(sql_command: insert_query, **options)
      @rows_inserted = result.cmd_tuples
    elsif (load_options == 'bulk_delete_append')
      result = query(sql_command: bulk_delete_from_main_query, **options)
      @rows_deleted = result.cmd_tuples
      result = query(sql_command: insert_query, **options)
      @rows_inserted = result.cmd_tuples
    elsif (load_options == 'truncate')
      query(sql_command: drop_main_query, **options)
      @rows_deleted = 0
      query(sql_command: rename_query, **options)
      @rows_inserted = query(sql_command:"SELECT COUNT(*) as row_count FROM #{schema}.#{table}").first['row_count'].to_i
    end

    query(sql_command: analyze_query, **options)

    self
  ensure
    query(sql_command: drop_staging_query, **options) if drop_staging_query
  end

  def set_copy(schema:, table:, **options)
    @schema = schema
    @table = table

    @load_options = options[:load_options]
    @primary_key = options[:primary_key]
    @bulk_delete_key_value = options[:bulk_delete_key_value]
    @delimiter = options[:delimiter]
    self
  end

  def copy(file:, schema:, table:, **options)
    copy_query = copy_command(schema, table)

    log("#{copy_query};", options[:verbose])

    enco = PG::TextEncoder::CopyRow.new
    conn.copy_data copy_query, enco do
      File.open(file,'rb') do |f|
        f.each_line do |line|
          conn.put_copy_data line.parse_csv
        end
      end
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
    @conn ||= PG::Connection.new(conn_payload)
  end

  def unmemoize_conn
    @conn = nil
  end

  def conn_payload
    {
      host: host,
      port: port,
      user: user,
      password: password,
      dbname: database_name
    }.delete_if {|k, v| v.nil?}
  end

  def copy_command(schema, table)
    "COPY #{schema}.#{table} FROM STDIN"
  end

  def stream_copy_to_staging_table(datasource)
    query(sql_command: create_staging_table_command(@schema, @table))

    copy_query = copy_command(@schema, staging_table(@table))

    coder = PG::TextEncoder::CopyRow.new

    conn.copy_data(copy_query, coder) do
      datasource.stream_download do |line|
        conn.put_copy_data line # already in the array format needed for put_copy_data
      end
    end
  end

  def contrived_upsert_from_staging_table
    # Contrived Upsert - DELETE and then INSERT - leaves some ghost records in 
    # scenarios where less records arrive to staging table than the main table
    delete_command = delete_from_main_table_command(@primary_key, @schema, @table)
    query(sql_command: delete_command) if delete_command
    insert_from_staging_table
    query(sql_command: analyze_table_command(@schema, @table))
  end

  def drop_main_table_and_rename_staging_table_to_main_table
    query(sql_command: drop_main_table_command(@schema, @table))
    query(sql_command: rename_table_staging_to_main_command(@schema, @table))
  end

  def bulk_delete_and_insert_from_staging_table
    query(sql_command: bulk_delete_from_main_table_command(@bulk_delete_key_value, @schema, @table))
    insert_from_staging_table
  end

  def insert_from_staging_table
    query(sql_command: insert_from_staging_table_command(@schema, @table))
  end
end
