# frozen_string_literal: true

require 'zlib'

require 'datamover/transformer/csv'
require 'datamover/datasource/shared/logger'

class FileDatasource
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :filepath, :decompress, :transformer

  def initialize(filepath:, decompress: false,
                 transformer: TC::Datamover::Transformer::Csv.new)
    self.filepath = filepath
    self.decompress = decompress
    self.transformer = transformer
  end

  def stream_download
    logger.info 'Starting read stream'

    reader = file
    reader = Zlib::GzipReader.new(file) if decompress

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
    logger.info 'Touching file'

    touch_file_with_path!

    logger.info 'Starting write stream'

    File.open(filepath, 'w') do |file|
      datasource.stream_download do |line|
        transformer.load_format(line) do |transformed_line|
          logger.debug transformed_line

          file.write transformed_line
        end
      end
    end

    logger.info 'Completed write stream'
  end

  private

  def file
    @file ||= File.new(filepath)
  end

  def touch_file_with_path!
    FileUtils.mkdir_p(dirname)
    FileUtils.touch(filepath)
  end

  def dirname
    File.dirname(filepath)
  end
end
