# frozen_string_literal: true
require 'spec_helper'
require 'dotenv/load'
require 'down'
require 'terminal-table'

describe 'Specialized Performance Test' do
  let(:download_url) do
    'https://archive.ics.uci.edu/ml/machine-learning-databases/00381/PRSA_data_2010.1.1-2014.12.31.csv' # real world 2MB file
  end

  it 'performs', performance: true do
    # Download the file locally
    tempfile = Down.download(download_url)

    byte_size = File.size(tempfile.path).to_f
    mb_size = byte_size / 2**20
    file_size = format('%.2f', mb_size)
    puts "File Size: #{file_size} MB"

    # File to S3
    datamover = TC::Datamover.new
    args = {
      filepath: tempfile.path
    }
    source = FileDatasource.new(args)
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }
    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result1.csv')

    function_name = 'File ⇒ S3'
    result1 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # S3 to S3
    datamover = TC::Datamover.new
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }
    source = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result1.csv')

    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }

    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result2.csv')

    function_name = 'S3 ⇒ S3'
    result2 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # S3 to Compressed S3
    datamover = TC::Datamover.new
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }
    source = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result2.csv')

    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME'],
      compress: true
    }

    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result3.csv.gz')

    function_name = 'S3 ⇒ Compressed S3'
    result3 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # Compressed S3 to Compressed S3
    datamover = TC::Datamover.new
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME'],
      decompress: true
    }
    source = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result3.csv.gz')

    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME'],
      compress: true
    }

    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result4.csv.gz')

    function_name = 'Compressed S3 ⇒ Compressed S3'
    result4 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # Compressed S3 to Decompressed S3
    datamover = TC::Datamover.new
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME'],
      decompress: true
    }
    source = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result4.csv.gz')

    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }

    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result5.csv')

    function_name = 'Compressed S3 ⇒ Decompressed S3'
    result5 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # Compressed S3 to Decompressed File
    datamover = TC::Datamover.new
    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME'],
      decompress: true
    }
    source = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result4.csv.gz')

    tempfile = Tempfile.new
    args = {
      filepath: tempfile.path
    }
    target = FileDatasource.new(args)

    function_name = 'Compressed S3 ⇒ Decompressed File'
    result6 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # File to SFTP
    # Download the file locally
    tempfile = Down.download(download_url)

    datamover = TC::Datamover.new
    args = {
      filepath: tempfile.path
    }
    source = FileDatasource.new(args)

    args = {
      host: ENV['SFTP_HOST'],
      login: ENV['SFTP_LOGIN'],
      password: ENV['SFTP_PASSWORD'],
      filepath: './ruby-datamover/spec/specialized_performance_spec/result7.csv',
      ssh_key: ENV['SFTP_SSH_KEY_PATH']
    }
    target = SftpDatasource.new(args)

    function_name = 'File ⇒ SFTP'
    result7 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # SFTP to S3
    datamover = TC::Datamover.new

    args = {
      host: ENV['SFTP_HOST'],
      login: ENV['SFTP_LOGIN'],
      password: ENV['SFTP_PASSWORD'],
      filepath: './ruby-datamover/spec/specialized_performance_spec/result7.csv',
      ssh_key: ENV['SFTP_SSH_KEY_PATH']
    }
    source = SftpDatasource.new(args)

    args = {
      access_key_id: ENV['S3_ACCESS_KEY_ID'],
      secret_access_key: ENV['S3_SECRET_ACCESS_KEY'],
      region: ENV['S3_REGION'],
      bucket_name: ENV['S3_BUCKET_NAME']
    }

    target = S3Datasource.new(args).set_filename('ruby-datamover/spec/specialized_performance_spec/result8.csv')

    function_name = 'SFTP ⇒ S3'
    result8 = time_and_memory_usage(function_name, file_size) do
      datamover.from_stream(source).to_stream(target)
    end

    # Setup and print table
    rows = []
    rows << ['Name', 'File (MB)', 'Mem (MB)', 'Time (Sec)'] # header row
    rows << :separator
    rows << result1
    rows << result2
    rows << result3
    rows << result4
    rows << result5
    rows << result6
    rows << result7
    rows << result8
    table = Terminal::Table.new(rows: rows)
    puts table
  end
end
