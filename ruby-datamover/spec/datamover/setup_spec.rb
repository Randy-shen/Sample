# frozen_string_literal: true
require 'spec_helper'

describe TC::Datasource::Setup do
  let(:instance) { TC::Datasource::Setup.new }

  describe '#datasources_config' do
    it 'returns the yaml parsed' do
      expect(instance.datasources_config).to_not eql(nil)
    end

    context 'when yaml_file does not exist' do
      it 'should write a warning out and return nil' do
        allow(instance).to receive(:yaml_file_exists?).and_return(false)

        expect(instance).to receive(:warn).with(instance.send(:path_warn_message))

        expect(instance.datasources_config).to eql(nil)
      end
    end
  end

  describe '#datamover_env' do
    it 'returns the ENV' do
      expect(instance.datamover_env).to eql('test')
    end

    context 'when datamover_env does not exist' do
      it 'should write a warning out and return nil' do
        allow(instance).to receive(:datamover_env_exists?).and_return(false)

        expect(instance).to receive(:warn)

        expect(instance.datamover_env).to eql(nil)
      end
    end
  end

  describe 'private methods' do
    describe '#yaml_file_exists?' do
      it 'returns true by default' do
        expect(instance.send(:yaml_file_exists?)).to eql(true)
      end

      context 'when datasource_path is empty string' do
        it 'returns false' do
          allow(instance).to receive(:datamover_datasource_path).and_return('')

          expect(instance.send(:yaml_file_exists?)).to eql(false)
        end
      end
    end
  end
end
