# frozen_string_literal: true
require 'zlib'

module Gzip
  def gzip_uncompress(buffer:)
    gz = Zlib::GzipReader.new(StringIO.new(buffer))
    gz.read
  end

  def gzip_compress(buffer:)
    gz = Zlib::GzipWriter.new(StringIO.new)

    gz << buffer

    gz.close.string
  end
end
