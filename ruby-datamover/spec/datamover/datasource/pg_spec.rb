# frozen_string_literal: true
require 'spec_helper'
require 'securerandom'

describe PgDatasource do
  let(:params) do
    {
      host: 'localhost',
      port: 9999,
      login: 'login',
      password: 'password'
    }
  end

  let!(:datasource) { described_class.new(params) }

  def pg_connection_exec
    count = instance_double('count')
    cmd_tuples = instance_double('cmd_tuples')
    exec = instance_double('exec', count: count, cmd_tuples: cmd_tuples)
  end

  def pg_connection_instance
    instance_double("PG::Connection", exec: pg_connection_exec, copy_data: true)
  end

  before do
    allow(PG::Connection).to receive(:new) { pg_connection_instance }
  end

  describe 'initialize' do
    it 'sets host' do
      expect(datasource.host).to eql('localhost')
    end

    it 'sets port' do
      expect(datasource.port).to eql(9999)
    end

    it 'sets user' do
      expect(datasource.user).to eql('login')
    end

    it 'sets password' do
      expect(datasource.password).to eql('password')
    end

    it 'optionally sets database_name' do
      params[:database_name] = 'app_test'
      datasource = described_class.new(params)
      expect(datasource.database_name).to eql('app_test')
    end
  end

  describe '#logger' do
    it 'has a logger' do
      expect(datasource.logger).to_not eql(nil)
      expect(datasource.logger.class).to eql(Logger)
    end
  end

  describe '#stream_download' do
    it 'does not implement' do
      expect do
        datasource.stream_download {|line| line}
      end.to raise_error(NotImplementedError)
    end
  end

  describe '#stream_upload' do
    context 'no load_options set' do
      it 'raises an error immediately' do
        datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: '')

        input_datasource = instance_double('input_datasource', stream_download: [])

        expect { datasource.stream_upload(input_datasource) }.to raise_error(DefaultDatasource::InvalidConfigurationValues)
      end
    end

    context 'insert' do
      it 'implements stream_upload block' do
        datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'insert')

        conn = datasource.send(:conn)
        input_datasource = instance_double('input_datasource', stream_download: [])

        expect(datasource).to receive(:stream_copy_to_staging_table)
        expect(datasource).to receive(:insert_from_staging_table)

        datasource.stream_upload(input_datasource)
      end
    end

    context 'when upsert' do
      it 'implements stream_upload block' do
        datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'upsert')

        conn = datasource.send(:conn)
        input_datasource = instance_double('input_datasource', stream_download: [])

        expect(datasource).to receive(:stream_copy_to_staging_table)
        expect(datasource).to receive(:contrived_upsert_from_staging_table)

        datasource.stream_upload(input_datasource)
      end
    end

    context 'bulk_delete_append' do
      it 'implements stream_upload block' do
        datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'bulk_delete_append')

        conn = datasource.send(:conn)
        input_datasource = instance_double('input_datasource', stream_download: [])

        expect(datasource).to receive(:stream_copy_to_staging_table)
        expect(datasource).to receive(:bulk_delete_and_insert_from_staging_table)

        datasource.stream_upload(input_datasource)
      end
    end

    context 'truncate' do
      it 'implements stream_upload block' do
        datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'truncate')

        conn = datasource.send(:conn)
        input_datasource = instance_double('input_datasource', stream_download: [])

        expect(datasource).to receive(:drop_main_table_and_rename_staging_table_to_main_table)

        datasource.stream_upload(input_datasource)
      end
    end
  end

  describe '#database' do
    it 'should reset the memoized connection' do
      original_conn = datasource.send(:conn)

      datasource.database('newdb')

      expect(datasource.send(:conn)).to_not eql(original_conn)
    end
  end

  describe '#set_query' do
    it 'sets @sql_command and returns its own instance' do
      expect(datasource.instance_variable_get(:@sql_command)).to eql(nil)

      result = datasource.set_query('SELECT * FROM somewhere')

      expect(result).to eql(datasource)
      expect(datasource.instance_variable_get(:@sql_command)).to eql('SELECT * FROM somewhere')
    end
  end

  describe '#set_copy' do
    it 'sets schema' do
      result = datasource.set_copy(schema: 'some-schema', table: 'some-table')

      expect(result.instance_variable_get(:@schema)).to eql('some-schema')
    end

    it 'sets table' do
      result = datasource.set_copy(schema: 'some-schema', table: 'some-table')

      expect(result.instance_variable_get(:@table)).to eql('some-table')
    end

    it 'sets load_options' do
      result = datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'truncate')

      expect(result.instance_variable_get(:@load_options)).to eql('truncate')

      result = datasource.set_copy(schema: 'some-schema', table: 'some-table', load_options: 'upsert')

      expect(result.instance_variable_get(:@load_options)).to eql('upsert')
    end
  end

  describe '#upload' do
    context 'when load_options is not a valid choice' do
      it 'raises an exception' do
        expect { datasource.upload(file: nil, load_options: 'invalid-option') }.to raise_error(DefaultDatasource::InvalidConfigurationValues)
      end
    end
      
    context 'when load_option is set elsewhere but still not a valid choice' do
      it 'raises an exception' do
        datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'invalid-option')
        expect { datasource.upload(file: nil) }.to raise_error(DefaultDatasource::InvalidConfigurationValues)
      end
    end

    it 'calls various of the sql query command generators' do
      expect(datasource).to receive(:create_staging_table_command)
      expect(datasource).to receive(:drop_staging_table_command)
      expect(datasource).to receive(:rename_table_staging_to_main_command)
      expect(datasource).to receive(:insert_from_staging_table_command)
      expect(datasource).to receive(:analyze_table_command)
      expect(datasource).to receive(:delete_from_main_table_command)
      expect(datasource).to receive(:bulk_delete_from_main_table_command)

      datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'insert')

      datasource.upload(file: nil)
    end

    it 'calls four queries' do
      datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'insert')

      expect(datasource).to receive(:query).and_return(pg_connection_exec).exactly(4).times

      datasource.upload(file: nil)
    end

    context 'when upsert' do
      it 'calls five queries' do
        datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'upsert')

        expect(datasource).to receive(:query).and_return(pg_connection_exec).exactly(5).times

        datasource.upload(file: nil)
      end
    end

    context 'when bulk_delete_append' do
      it 'calls five queries' do
        datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'bulk_delete_append')

        expect(datasource).to receive(:query).and_return(pg_connection_exec).exactly(5).times

        datasource.upload(file: nil)
      end
    end

    it 'calls copy command' do
      datasource.set_copy(schema: 'dbo', table: 'vinz', load_options: 'bulk_delete_append')

      expect(datasource).to receive(:copy).once

      datasource.upload(file: nil)
    end
  end

  describe '#get_display_name' do
    context 'when get_file_path is empty' do
      it 'generates a name combining the schema table and host' do
        datasource.instance_variable_set(:@schema, 'some-schema')
        datasource.instance_variable_set(:@table, 'some-table')

        expect(datasource.get_display_name).to eql('localhost//some-schema.some-table')
      end
    end
  end

  describe 'private methods' do
    describe '#conn_payload' do
      context 'when no database' do
        it 'returns correct payload' do
          expect(datasource.send(:conn_payload)).to eql({
            host: 'localhost',
            port: 9999,
            user: 'login',
            password: 'password'
          })
        end
      end

      context 'when database is set' do
        it 'returns payload with dbname' do
          params[:database_name] = 'database'

          datasource = described_class.new(params)

          expect(datasource.send(:conn_payload)).to eql({
            host: 'localhost',
            port: 9999,
            user: 'login',
            password: 'password',
            dbname: 'database'
          })
        end
      end
    end

    describe '#available_load_options' do
      it 'returns the correct options' do
        expect(datasource.send(:available_load_options)).to eql(%w(insert upsert bulk_delete_append truncate))
      end
    end

    describe '#staging_suffix' do
      it 'returns a shortend version' do
        expect(datasource.send(:staging_suffix)).to_not eql(nil)
      end

      it 'should not change when accessed again (unless a new instance)' do
        former = datasource.send(:staging_suffix)
        latter = datasource.send(:staging_suffix)

        expect(former).to eql(latter)

        datasource = RedshiftDatasource.new(params)
        latter = datasource.send(:staging_suffix)
        expect(former).to_not eql(latter)
      end
    end

    describe '#staging_table' do
      it 'returns a value with the table in it' do
        datasource.instance_variable_set(:@table, 'some-table')

        expect(datasource.send(:staging_table)).to include('some-table')
      end

      it 'can have an overridden value' do
        datasource.instance_variable_set(:@table, 'some-table')

        expect(datasource.send(:staging_table, 'override')).to include('override')
      end
    end

    describe '#create_staging_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:create_staging_table_command, 'dbo', 'vins')
        staging_tbl = datasource.send(:staging_table, 'vins')
        command = "CREATE TABLE dbo.#{staging_tbl} ( LIKE dbo.vins );"

        expect(result).to eql(command)
      end
    end

    describe '#drop_staging_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:drop_staging_table_command, 'dbo', 'vins')
        staging_tbl = datasource.send(:staging_table, 'vins')

        command = "DROP TABLE IF EXISTS dbo.#{staging_tbl};"

        expect(result).to eql(command)
      end
    end

    describe '#drop_main_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:drop_main_table_command, 'dbo', 'vins')

        command = "DROP TABLE IF EXISTS dbo.vins;"

        expect(result).to eql(command)
      end
    end

    describe '#rename_table_staging_to_main_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:rename_table_staging_to_main_command, 'dbo', 'vins')

        staging_tbl = datasource.send(:staging_table, 'vins')
        command = "ALTER TABLE dbo.#{staging_tbl} RENAME TO vins;"

        expect(result).to eql(command)
      end
    end

    describe '#insert_from_staging_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:insert_from_staging_table_command, 'dbo', 'vins')

        staging_tbl = datasource.send(:staging_table, 'vins')
        command = "INSERT INTO dbo.vins SELECT * FROM dbo.#{staging_tbl};"

        expect(result).to eql(command)
      end
    end

    describe '#analyze_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:analyze_table_command, 'dbo', 'vins')
        command = 'ANALYZE dbo.vins;'

        expect(result).to eql(command)
      end
    end

    describe '#delete_from_main_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:delete_from_main_table_command, 'id,style_id', 'dbo', 'vins')
        staging_tbl = datasource.send(:staging_table, 'vins')

        command = "DELETE FROM dbo.vins USING dbo.#{staging_tbl} WHERE dbo.vins.id = dbo.#{staging_tbl}.id AND dbo.vins.style_id = dbo.#{staging_tbl}.style_id;"

        expect(result).to eql(command)
      end

      context 'when missing primary key' do
        it 'returns nil' do
          result = datasource.send(:delete_from_main_table_command, nil, 'dbo', 'vins')

          expect(result).to eql(nil)
        end
      end

      context 'when primary key is a blank string' do
        it 'returns nil' do
          result = datasource.send(:delete_from_main_table_command, ' ', 'dbo', 'vins')

          expect(result).to eql(nil)
        end
      end
    end

    describe '#bulk_delete_from_main_table_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:bulk_delete_from_main_table_command, { position: 1, color: 'blue' },  'dbo', 'vins')
        command = "DELETE FROM dbo.vins WHERE position LIKE '1' AND color LIKE 'blue';"

        expect(result).to eql(command)
      end

      context 'when missing bulk_delete_key_value' do
        it 'returns nil' do
          result = datasource.send(:bulk_delete_from_main_table_command, nil, 'dbo', 'vins')

          expect(result).to eql(nil)
        end
      end

      context 'when bulk_delete_key_value is an empty hash' do
        it 'returns nil' do
          result = datasource.send(:bulk_delete_from_main_table_command, {}, 'dbo', 'vins')

          expect(result).to eql(nil)
        end
      end
    end

    describe '#copy_command' do
      it 'generates the correct sql statement' do
        result = datasource.send(:copy_command, 'dbo', 'vins')
        command = "COPY dbo.vins FROM STDIN"

        expect(result).to eql(command)
      end
    end

    describe '#log' do
      it 'appends to the @logs instance variable' do
        datasource.send(:log, 'one')
        datasource.send(:log, 'two')

        expect(datasource.instance_variable_get(:@logs)).to eql(%w(one two))

        datasource.send(:log, 'SOME SELECT STATEMENT')

        expect(datasource.instance_variable_get(:@logs)).to eql(['one', 'two', 'SOME SELECT STATEMENT'])
      end

      context 'when verbose mode is set' do
        it 'sends data to stdout' do
          expect(STDOUT).to receive(:puts).with('one')
          datasource.send(:log, 'one', 1)

          expect(STDOUT).to receive(:puts).with('SOME SELECT STATEMENT')
          datasource.send(:log, 'SOME SELECT STATEMENT', 1)

          expect(STDOUT).to_not receive(:puts).with('three')
          datasource.send(:log, 'three')

          expect(STDOUT).to_not receive(:puts).with('four')
          datasource.send(:log, 'four', 0)
        end
      end
    end

    describe '#contrived_upsert_from_staging_table' do
      let(:schema) { '[dbo]' }
      let(:table) { 'some_table' }
      let(:primary_key) { 'style_id' }

      it 'runs delete, insert, and analyze' do
        datasource.set_copy(schema: schema, table: table, primary_key: primary_key)

        sql_command1 = datasource.send(:delete_from_main_table_command, primary_key, schema, table)
        sql_command2 = datasource.send(:insert_from_staging_table_command, schema, table)
        sql_command3 = datasource.send(:analyze_table_command, schema, table)

        expect(datasource).to receive(:query).with({sql_command: sql_command1}).once
        expect(datasource).to receive(:query).with({sql_command: sql_command2}).once
        expect(datasource).to receive(:query).with({sql_command: sql_command3}).once

        datasource.send(:contrived_upsert_from_staging_table)
      end

      context 'when primary key is not set' do
        it 'does not execute the delete sql command' do
          datasource.set_copy(schema: schema, table: table, primary_key: nil)

          sql_command1 = datasource.send(:delete_from_main_table_command, primary_key, schema, table)
          sql_command2 = datasource.send(:insert_from_staging_table_command, schema, table)
          sql_command3 = datasource.send(:analyze_table_command, schema, table)

          expect(datasource).to_not receive(:query).with({sql_command: sql_command1})
          expect(datasource).to receive(:query).with({sql_command: sql_command2}).once
          expect(datasource).to receive(:query).with({sql_command: sql_command3}).once

          datasource.send(:contrived_upsert_from_staging_table)
        end
      end
    end

    describe '#drop_main_table_and_rename_staging_table_to_main_table' do
      let(:schema) { '[dbo]' }
      let(:table) { 'some_table' }
      let(:primary_key) { 'style_id' }

      it 'runs drop and rename' do
        datasource.set_copy(schema: schema, table: table, primary_key: primary_key)

        sql_command1 = datasource.send(:drop_main_table_command, schema, table)
        sql_command2 = datasource.send(:rename_table_staging_to_main_command, schema, table)

        expect(datasource).to receive(:query).with({sql_command: sql_command1}).once
        expect(datasource).to receive(:query).with({sql_command: sql_command2}).once

        datasource.send(:drop_main_table_and_rename_staging_table_to_main_table)
      end
    end

    describe '#bulk_delete_and_insert_from_staging_table' do
      let(:schema) { '[dbo]' }
      let(:table) { 'some_table' }
      let(:primary_key) { 'style_id' }
      let(:load_options) { 'bulk_delete_append' }
      let(:bulk_delete_key_value) do
        {
          'effective_date': '2017-05-01',
          'country': 'US'
        }
      end

      it 'runs drop and rename' do
        datasource.set_copy(schema: schema, table: table, bulk_delete_key_value: bulk_delete_key_value, load_options: load_options)

        sql_command1 = datasource.send(:bulk_delete_from_main_table_command, bulk_delete_key_value, schema, table)
        sql_command2 = datasource.send(:insert_from_staging_table_command, schema, table)

        expect(datasource).to receive(:query).with({sql_command: sql_command1}).once
        expect(datasource).to receive(:query).with({sql_command: sql_command2}).once

        datasource.send(:bulk_delete_and_insert_from_staging_table)
      end
    end

    describe '#stream_copy_to_staging_table' do
      let(:schema) { '[dbo]' }
      let(:table) { 'some_table' }
      let(:primary_key) { 'style_id' }

      it 'calls copy_data with the correct orgs' do
        datasource.set_copy(schema: schema, table: table, primary_key: primary_key)

        input_datasource = instance_double('input_datasource', stream_download: [])
        conn = datasource.send(:conn)

        copy_query = datasource.send(:copy_command, schema, datasource.send(:staging_table, table))
        coder = PG::TextEncoder::CopyRow.new

        expect(conn).to receive(:copy_data).with(copy_query, coder)

        sql_command0 = datasource.send(:create_staging_table_command, schema, table)
        expect(datasource).to receive(:query).with({sql_command: sql_command0}).once

        datasource.send(:stream_copy_to_staging_table, input_datasource)
      end
    end

    describe '#staging_suffix' do
      it 'should use a unix timestamp' do
        specified_time = Time.utc(2017, 05, 26, 8, 0)

        Timecop.freeze(specified_time) do
          result = datasource.send(:staging_suffix)

          expect(result.length).to eql(16)

          expect(result).to start_with(specified_time.to_i.to_s)
        end
      end

      it 'should include a 5 digit int random identifier' do
        result = datasource.send(:staging_suffix)

        random_identifier = result.split('_')[-1]

        expect(random_identifier.length).to eql(5)
        expect(random_identifier.to_i).to be > 0
      end
    end
  end
end
