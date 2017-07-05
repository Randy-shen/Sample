# frozen_string_literal: true
require 'spec_helper'
require 'securerandom'

describe SqlServerDatasource do
  let(:params) do
    {
      host: 'localhost',
      login: 'login',
      password: 'password'
    }
  end

  let!(:datasource) { described_class.new(params) }

  def sqlserver_connection_execute
    count = instance_double('count')
    cmd_tuples = instance_double('cmd_tuples')
    execute = instance_double('execute', count: count, cmd_tuples: cmd_tuples)
  end

  def sqlserver_connection_instance
    instance_double("TinyTds::Client", execute: sqlserver_connection_execute)
  end

  before do
    allow(TinyTds::Client).to receive(:new) { sqlserver_connection_instance }
  end

  describe 'initialize' do
    it 'sets host' do
      expect(datasource.host).to eql('localhost')
    end

    it 'sets username' do
      expect(datasource.username).to eql('login')
    end

    it 'sets password' do
      expect(datasource.password).to eql('password')
    end

    it 'optionally sets database_name' do
      params[:database_name] = 'app_test'
      datasource = described_class.new(params)
      expect(datasource.database_name).to eql('app_test')
    end

    it 'optionally sets database_name the deprecated way' do
      params[:database] = 'app_test'
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
    it 'calls file.each_line block' do
      tds_result = datasource.send(:tds_result)

      expect(tds_result).to receive(:each)

      datasource.stream_download { |line| line }
    end
  end

  describe '#stream_upload' do
    it 'does not implement' do
      input_datasource = instance_double('input_datasource', stream_download: [])
      expect do
        datasource.stream_upload(input_datasource)
      end.to raise_error(NotImplementedError)
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
    describe '#tds_result' do
      it 'returns call query with sql command' do
        sql_command = datasource.instance_variable_get(:@sql_command)

        expect(datasource).to receive(:query).with(sql_command: sql_command)

        datasource.send(:tds_result)
      end
    end

    describe '#conn_payload' do
      context 'when no database' do
        it 'returns correct payload' do
          expect(datasource.send(:conn_payload)).to eql({
            host: 'localhost',
            username: 'login',
            password: 'password',
            timeout: 0
          })
        end
      end

      context 'when database is set' do
        it 'returns payload with dbname' do
          params[:database] = 'database'

          datasource = described_class.new(params)

          expect(datasource.send(:conn_payload)).to eql({
            host: 'localhost',
            username: 'login',
            password: 'password',
            timeout: 0,
            database: 'database'
          })
        end
      end
    end

    describe '#clean_invisible_characters' do
      it 'cleans newline characters' do
        line = %q(ANN201703012017410358160,US,ANN,2017-03-01 00:00:00 +0000,2017-01-15 00:00:00 +0000,2017,KIA,OPTIMA,4dr Sdn EX PHEV,410,358,160,1.0,Midsize,145,Optima,Optima,14732,36105,,0,39600,4,4,AOSSSOSSSSS  O S S  S SS ,44,16,160,410358160,4416160,X5242,Mainstream,China,35210.0,32743.0,895.0,37549.2,,1,2019,,^M
2.0L 4 Cyl,^M 2.0L ,4 Cyl,,1,,,,36100,29000,29000,32555,150.0,true)
        record = line.split(',') # simulates record from tds_result

        correct_line = %q(ANN201703012017410358160,US,ANN,2017-03-01 00:00:00 +0000,2017-01-15 00:00:00 +0000,2017,KIA,OPTIMA,4dr Sdn EX PHEV,410,358,160,1.0,Midsize,145,Optima,Optima,14732,36105,,0,39600,4,4,AOSSSOSSSSS  O S S  S SS ,44,16,160,410358160,4416160,X5242,Mainstream,China,35210.0,32743.0,895.0,37549.2,,1,2019,,^M 2.0L 4 Cyl,^M 2.0L ,4 Cyl,,1,,,,36100,29000,29000,32555,150.0,true)
        correct_record = correct_line.split(',')

        cleansed = datasource.send(:clean_invisible_characters, record)

        correct_record.each_with_index do |value, index|
          expect(cleansed[index]).to eql(value)
        end
      end

      it 'can handle non string values ok' do
        record = [1, Time.now.utc]

        cleansed = datasource.send(:clean_invisible_characters, record)

        expect(cleansed).to eql(record)
      end

      it 'performs well', performance: true do
        line = %q(ANN201703012017410358160,US,ANN,2017-03-01 00:00:00 +0000,2017-01-15 00:00:00 +0000,2017,KIA,OPTIMA,4dr Sdn EX PHEV,410,358,160,1.0,Midsize,145,Optima,Optima,14732,36105,,0,39600,4,4,AOSSSOSSSSS  O S S  S SS ,44,16,160,410358160,4416160,X5242,Mainstream,China,35210.0,32743.0,895.0,37549.2,,1,2019,,^M
2.0L 4 Cyl,^M 2.0L ,4 Cyl,,1,,,,36100,29000,29000,32555,150.0,true)
        record = line.split(',') # simulates record from tds_result

        expect do
          1_000.times do
            datasource.send(:clean_invisible_characters, record)
          end
        end.to perform_under(40).ms
      end

      it 'does ok with memory', memory: true do
        line = %q(ANN201703012017410358160,US,ANN,2017-03-01 00:00:00 +0000,2017-01-15 00:00:00 +0000,2017,KIA,OPTIMA,4dr Sdn EX PHEV,410,358,160,1.0,Midsize,145,Optima,Optima,14732,36105,,0,39600,4,4,AOSSSOSSSSS  O S S  S SS ,44,16,160,410358160,4416160,X5242,Mainstream,China,35210.0,32743.0,895.0,37549.2,,1,2019,,^M
2.0L 4 Cyl,^M 2.0L ,4 Cyl,,1,,,,36100,29000,29000,32555,150.0,true)
        record = line.split(',') # simulates record from tds_result

        report = MemoryProfiler.report do
          1_000.times do
            datasource.send(:clean_invisible_characters, record)
          end
        end

        report.pretty_print
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
