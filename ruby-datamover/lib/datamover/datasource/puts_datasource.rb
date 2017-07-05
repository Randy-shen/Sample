# frozen_string_literal: true

require 'datamover/transformer/csv'
require 'datamover/datasource/shared/logger'

class PutsDatasource
  include TC::Datamover::Datasource::Shared::Logger

  attr_accessor :transformer

  def initialize(transformer: TC::Datamover::Transformer::Csv.new)
    self.transformer = transformer
  end

  def stream_download
    msg = 'does not implement stream_download'

    logger.fatal msg

    raise NotImplementedError, msg
  end

  def stream_upload(datasource)
    logger.info 'Starting write stream'

    datasource.stream_download do |line|
      transformer.load_format(line) do |transformed_line|
        logger.debug transformed_line

        puts transformed_line
      end
    end

    logger.info 'Completed write stream'
  end
end
