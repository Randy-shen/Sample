# frozen_string_literal: true
require 'spec_helper'

describe PutsDatasource do
  let(:filepath) { File.join(File.dirname(__FILE__), '..', '..', 'files', 'data.csv') }

  let!(:datasource) { described_class.new }

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
    it 'implements stream_download block' do
      input_datasource = instance_double('input_datasource', stream_download: [])

      expect(input_datasource).to receive(:stream_download)

      datasource.stream_upload(input_datasource)
    end
  end
end
