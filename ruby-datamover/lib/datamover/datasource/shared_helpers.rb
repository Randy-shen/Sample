module SharedHelpers
  def validate_load_option!(load_option)
    unless available_load_options.include?(load_option)
      message = "{load_options: #{load_option}} is not a valid configuration. {load_options: (#{available_load_options.join('|')})}"

      raise DefaultDatasource::InvalidConfigurationValues, message
    end
  end

  def available_load_options
    %w(insert upsert bulk_delete_append truncate)
  end

  def staging_suffix
    @staging_suffix ||= [Time.now.to_i, rand.to_s[2..6]].join('_') # use unix timestamp for debugging purposes and random to give likely uniqueness
  end

  def staging_table(table_name = @table)
    "#{table_name}_#{staging_suffix}"
  end

  def create_staging_table_command(schema, table)
    "CREATE TABLE #{schema}.#{staging_table(table)} ( LIKE #{schema}.#{table} );"
  end

  def drop_staging_table_command(schema, table)
    "DROP TABLE IF EXISTS #{schema}.#{staging_table(table)};"
  end

  def drop_main_table_command(schema, table)
    "DROP TABLE IF EXISTS #{schema}.#{table};"
  end

  def rename_table_staging_to_main_command(schema, table)
    "ALTER TABLE #{schema}.#{staging_table(table)} RENAME TO #{table};"
  end

  def insert_from_staging_table_command(schema, table)
    "INSERT INTO #{schema}.#{table} SELECT * FROM #{schema}.#{staging_table(table)};"
  end
  
  def analyze_table_command(schema, table)
    "ANALYZE #{schema}.#{table};"
  end

  def delete_from_main_table_command(primary_key, schema, table)
    return nil if primary_key.to_s.strip.empty?

    key_compare = primary_key.split(',').map do |key|
      " AND #{schema}.#{table}.#{key.strip} = #{schema}.#{staging_table(table)}.#{key.strip}"
    end.join[4..-1]

    "DELETE FROM #{schema}.#{table} USING #{schema}.#{staging_table(table)} WHERE #{key_compare.strip};"
  end

  def bulk_delete_from_main_table_command(bulk_delete_key_value, schema, table)
    return nil if bulk_delete_key_value.to_s.strip.empty? || bulk_delete_key_value.empty?

    key_compare = bulk_delete_key_value.map{|key,value| " AND #{key.to_s.strip} LIKE '#{value.to_s.strip}'"}.join[4..-1]

    "DELETE FROM #{schema}.#{table} WHERE #{key_compare.strip};"
  end

  def log(text, verbose = 0)
    puts text if verbose == 1

    @logs.push(text)
  end
end
