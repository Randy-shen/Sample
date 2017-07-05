# frozen_string_literal: true

require 'csv'
require 'pg'

require 'datamover/datasource/shared/logger'

class RedshiftDatasource < PgDatasource
  include TC::Datamover::Datasource::Shared::Logger

	attr_reader :s3url

  def stream_download
    raise NotImplementedError, 'does not yet implement stream_download'
  end

  def upload(s3url:, access_key_id:, secret_access_key:, schema:@schema, table:@table, load_options:@load_options, primary_key:@primary_key, bulk_delete_key_value:@bulk_delete_key_value, **options)
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

    copy(s3url:s3url, access_key_id:access_key_id, secret_access_key:secret_access_key, schema:schema, table:staging_table(table), **options)

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

  def copy(s3url:, access_key_id:, secret_access_key:, schema:, table:, **options)
    @s3url = s3url

    log("s3url from redshift is #{s3url}", 1)

    copy_query = internal_amazon_copy_command(schema, table, s3url, access_key_id, secret_access_key, @delimiter, **options)

    self.query(sql_command: copy_query, **options)
  end

  private

  def internal_amazon_copy_command(schema, table, s3url, access_key_id, secret_access_key, delimiter = nil, **options)
    output = "COPY #{schema}.#{table} FROM '#{s3url}' CREDENTIALS 'aws_access_key_id=#{access_key_id};aws_secret_access_key=#{secret_access_key}' CSV ACCEPTANYDATE BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS"
    output += " GZIP" if options[:gzip]
    output += " COMPUPDATE ON"
    output += " DELIMITER '#{delimiter}'" if delimiter
    output += ';'
  end
end
