# frozen_string_literal: true
require 'spec_helper'

describe Gzip do
  let(:data) do
    %(
      Lorem Ipsum is simply dummy text of the printing and typesetting industry.
      Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
      when an unknown printer took a galley of type and scrambled it to make a type
      specimen book. It has survived not only five centuries, but also the leap
      into electronic typesetting, remaining essentially unchanged. It was
      popularised in the 1960s with the release of Letraset sheets containing
      Lorem Ipsum passages, and more recently with desktop publishing software
      like Aldus PageMaker including versions of Lorem Ipsum.
    )
  end

  let(:compressed_data) do
    gz = Zlib::GzipWriter.new(StringIO.new)
    gz << data
    gz.close.string
  end

  let(:compress_args) do
    {
      buffer: data
    }
  end

  let(:uncompress_args) do
    {
      buffer: compressed_data
    }
  end

  let(:helpers) { Object.new.extend(Gzip) }

  describe '#gzip_compress' do
    it 'compresses a given string' do
      result = helpers.gzip_compress(compress_args)

      uncompressed = Zlib::GzipReader.new(StringIO.new(result))

      expect(uncompressed.read).to eql(data)
    end
  end

  describe '#gzip_uncompress' do
    it 'uncompresses gzipped data' do
      result = helpers.gzip_uncompress(uncompress_args)

      expect(result).to eql(data)
    end
  end
end
